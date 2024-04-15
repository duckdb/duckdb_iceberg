#include "duckdb.hpp"
#include "iceberg_metadata.hpp"
#include "iceberg_utils.hpp"
#include "iceberg_types.hpp"

#include "avro/Compiler.hh"
#include "avro/DataFile.hh"
#include "avro/Decoder.hh"
#include "avro/Encoder.hh"
#include "avro/ValidSchema.hh"
#include "avro/Stream.hh"

namespace duckdb {

IcebergTable IcebergTable::Load(const string &iceberg_path, IcebergSnapshot &snapshot, FileSystem &fs,
                                bool allow_moved_paths, string metadata_compression_codec) {
	IcebergTable ret;
	ret.path = iceberg_path;
	ret.snapshot = snapshot;

	auto manifest_list_full_path = allow_moved_paths
	                                   ? IcebergUtils::GetFullPath(iceberg_path, snapshot.manifest_list, fs)
	                                   : snapshot.manifest_list;
	auto manifests = ReadManifestListFile(manifest_list_full_path, fs, snapshot.iceberg_format_version);

	for (auto &manifest : manifests) {
		auto manifest_entry_full_path = allow_moved_paths
		                                    ? IcebergUtils::GetFullPath(iceberg_path, manifest.manifest_path, fs)
		                                    : manifest.manifest_path;
		auto manifest_paths = ReadManifestEntries(manifest_entry_full_path, fs, snapshot.iceberg_format_version);

		ret.entries.push_back({std::move(manifest), std::move(manifest_paths)});
	}

	return ret;
}

vector<IcebergManifest> IcebergTable::ReadManifestListFile(const string &path, FileSystem &fs, idx_t iceberg_format_version) {
	vector<IcebergManifest> ret;

	// TODO: make streaming
	string file = IcebergUtils::FileToString(path, fs);

	auto stream = avro::memoryInputStream((unsigned char *)file.c_str(), file.size());
	avro::ValidSchema schema;

	if (iceberg_format_version == 1) {
		schema = avro::compileJsonSchemaFromString(MANIFEST_SCHEMA_V1);
		avro::DataFileReader<c::manifest_file_v1> dfr(std::move(stream), schema);
		c::manifest_file_v1 manifest_list;
		while (dfr.read(manifest_list)) {
			ret.emplace_back(IcebergManifest(manifest_list));
		}
	} else {
		schema = avro::compileJsonSchemaFromString(MANIFEST_SCHEMA);
		avro::DataFileReader<c::manifest_file> dfr(std::move(stream), schema);
		c::manifest_file manifest_list;
		while (dfr.read(manifest_list)) {
			ret.emplace_back(IcebergManifest(manifest_list));
		}
	}

	return ret;
}

vector<IcebergManifestEntry> IcebergTable::ReadManifestEntries(const string &path, FileSystem &fs,
                                                               idx_t iceberg_format_version) {
	vector<IcebergManifestEntry> ret;

	// TODO: make streaming
	string file = IcebergUtils::FileToString(path, fs);
	auto stream = avro::memoryInputStream((unsigned char *)file.c_str(), file.size());

	if (iceberg_format_version == 1) {
		auto schema = avro::compileJsonSchemaFromString(MANIFEST_ENTRY_SCHEMA_V1);
		avro::DataFileReader<c::manifest_entry_v1> dfr(std::move(stream), schema);
		c::manifest_entry_v1 manifest_entry;
		while (dfr.read(manifest_entry)) {
			ret.emplace_back(IcebergManifestEntry(manifest_entry));
		}
	} else {
		auto schema = avro::compileJsonSchemaFromString(MANIFEST_ENTRY_SCHEMA);
		avro::DataFileReader<c::manifest_entry> dfr(std::move(stream), schema);
		c::manifest_entry manifest_entry;
		while (dfr.read(manifest_entry)) {
			ret.emplace_back(IcebergManifestEntry(manifest_entry));
		}
	}

	return ret;
}

unique_ptr<SnapshotParseInfo> IcebergSnapshot::GetParseInfo(yyjson_doc &metadata_json) {
	SnapshotParseInfo info {};
	auto root = yyjson_doc_get_root(&metadata_json);
	info.iceberg_version = IcebergUtils::TryGetNumFromObject(root, "format-version");
	info.snapshots = yyjson_obj_get(root, "snapshots");

	// Multiple schemas can be present in the json metadata 'schemas' list
	if (yyjson_obj_getn(root, "current-schema-id", string("current-schema-id").size())) {
		size_t idx, max;
		yyjson_val *schema;
		info.schema_id = IcebergUtils::TryGetNumFromObject(root, "current-schema-id");
		auto schemas = yyjson_obj_get(root, "schemas");
		yyjson_arr_foreach(schemas, idx, max, schema) {
			info.schemas.push_back(schema);
		}
	} else {
		auto schema = yyjson_obj_get(root, "schema");
		if (!schema) {
			throw IOException("Neither a valid schema or schemas field was found");
		}
		auto found_schema_id = IcebergUtils::TryGetNumFromObject(schema, "schema-id");
		info.schemas.push_back(schema);
		info.schema_id = found_schema_id;
	}

	return make_uniq<SnapshotParseInfo>(std::move(info));
}

unique_ptr<SnapshotParseInfo> IcebergSnapshot::GetParseInfo(const string &path, FileSystem &fs, string metadata_compression_codec) {
	auto metadata_json = ReadMetaData(path, fs, metadata_compression_codec);
	auto doc = yyjson_read(metadata_json.c_str(), metadata_json.size(), 0);
	auto parse_info = GetParseInfo(*doc);

	// Transfer string and yyjson doc ownership
	parse_info->doc = doc;
	parse_info->document = std::move(metadata_json);

	return parse_info;
}

IcebergSnapshot IcebergSnapshot::GetLatestSnapshot(const string &path, FileSystem &fs,
	string metadata_compression_codec, bool skip_schema_inference) {
	auto info = GetParseInfo(path, fs, metadata_compression_codec);
	auto latest_snapshot = FindLatestSnapshotInternal(info->snapshots);

	if (!latest_snapshot) {
		throw IOException("No snapshots found");
	}

	return ParseSnapShot(latest_snapshot, info->iceberg_version, info->schema_id, info->schemas, metadata_compression_codec, skip_schema_inference);
}

IcebergSnapshot IcebergSnapshot::GetSnapshotById(const string &path, FileSystem &fs, idx_t snapshot_id,
	string metadata_compression_codec, bool skip_schema_inference) {
	auto info = GetParseInfo(path, fs, metadata_compression_codec);
	auto snapshot = FindSnapshotByIdInternal(info->snapshots, snapshot_id);

	if (!snapshot) {
		throw IOException("Could not find snapshot with id " + to_string(snapshot_id));
	}

	return ParseSnapShot(snapshot, info->iceberg_version, info->schema_id, info->schemas,
		metadata_compression_codec, skip_schema_inference);
}

IcebergSnapshot IcebergSnapshot::GetSnapshotByTimestamp(const string &path, FileSystem &fs, timestamp_t timestamp, string metadata_compression_codec,
	bool skip_schema_inference) {
	auto info = GetParseInfo(path, fs, metadata_compression_codec);
	auto snapshot = FindSnapshotByIdTimestampInternal(info->snapshots, timestamp);

	if (!snapshot) {
		throw IOException("Could not find latest snapshots for timestamp " + Timestamp::ToString(timestamp));
	}

	return ParseSnapShot(snapshot, info->iceberg_version, info->schema_id, info->schemas, metadata_compression_codec, skip_schema_inference);
}

// Function to generate a metadata file url
string GenerateMetaDataUrl(FileSystem &fs, const string &meta_path, const string &table_version, const string &metadata_compression_codec) {
	if (metadata_compression_codec != "gzip") {
		return fs.JoinPath(meta_path, "v" + table_version + ".metadata.json");
	}
	return fs.JoinPath(meta_path, "v" + table_version + ".gz.metadata.json");
}

string IcebergSnapshot::ReadMetaData(const string &path, FileSystem &fs, string metadata_compression_codec) {
	string metadata_file_path;
	if (StringUtil::EndsWith(path, ".json")) {
		metadata_file_path = path;
	} else {
		auto table_version = GetTableVersion(path, fs);
		auto meta_path = fs.JoinPath(path, "metadata");
		metadata_file_path = GenerateMetaDataUrl(fs, meta_path, table_version, metadata_compression_codec);
	}

	if (metadata_compression_codec == "gzip") {
		return IcebergUtils::GzFileToString(metadata_file_path, fs);
	}
	return IcebergUtils::FileToString(metadata_file_path, fs);
}

IcebergSnapshot IcebergSnapshot::ParseSnapShot(yyjson_val *snapshot, idx_t iceberg_format_version, idx_t schema_id,
                                               vector<yyjson_val *> &schemas, string metadata_compression_codec,
											   bool skip_schema_inference) {
	IcebergSnapshot ret;
	auto snapshot_tag = yyjson_get_tag(snapshot);
	if (snapshot_tag != YYJSON_TYPE_OBJ) {
		throw IOException("Invalid snapshot field found parsing iceberg metadata.json");
	}
	ret.metadata_compression_codec = metadata_compression_codec;
	if (iceberg_format_version == 1) {
		ret.sequence_number = 0;
	} else if (iceberg_format_version == 2) {
		ret.sequence_number = IcebergUtils::TryGetNumFromObject(snapshot, "sequence-number");
	}

	ret.snapshot_id = IcebergUtils::TryGetNumFromObject(snapshot, "snapshot-id");
	ret.timestamp_ms = Timestamp::FromEpochMs(IcebergUtils::TryGetNumFromObject(snapshot, "timestamp-ms"));
	ret.manifest_list = IcebergUtils::TryGetStrFromObject(snapshot, "manifest-list");
	ret.iceberg_format_version = iceberg_format_version;
	ret.schema_id = schema_id;
	if (!skip_schema_inference) {
		ret.schema = ParseSchema(schemas, ret.schema_id);
	}
	return ret;
}

string IcebergSnapshot::GetTableVersion(const string &path, FileSystem &fs) {
	auto meta_path = fs.JoinPath(path, "metadata");
	auto version_file_path = fs.JoinPath(meta_path, "version-hint.text");
	auto version_file_content = IcebergUtils::FileToString(version_file_path, fs);

	try {
		return version_file_content;
	} catch (std::invalid_argument &e) {
		throw IOException("Iceberg version hint file contains invalid value");
	} catch (std::out_of_range &e) {
		throw IOException("Iceberg version hint file contains invalid value");
	}
}

yyjson_val *IcebergSnapshot::FindLatestSnapshotInternal(yyjson_val *snapshots) {
	size_t idx, max;
	yyjson_val *snapshot;

	uint64_t max_timestamp = NumericLimits<uint64_t>::Minimum();
	yyjson_val *max_snapshot = nullptr;

	yyjson_arr_foreach(snapshots, idx, max, snapshot) {

		auto timestamp = IcebergUtils::TryGetNumFromObject(snapshot, "timestamp-ms");
		if (timestamp >= max_timestamp) {
			max_timestamp = timestamp;
			max_snapshot = snapshot;
		}
	}

	return max_snapshot;
}

yyjson_val *IcebergSnapshot::FindSnapshotByIdInternal(yyjson_val *snapshots, idx_t target_id) {
	size_t idx, max;
	yyjson_val *snapshot;

	yyjson_arr_foreach(snapshots, idx, max, snapshot) {

		auto snapshot_id = IcebergUtils::TryGetNumFromObject(snapshot, "snapshot-id");

		if (snapshot_id == target_id) {
			return snapshot;
		}
	}

	return nullptr;
}

yyjson_val *IcebergSnapshot::IcebergSnapshot::FindSnapshotByIdTimestampInternal(yyjson_val *snapshots,
                                                                                timestamp_t timestamp) {
	size_t idx, max;
	yyjson_val *snapshot;

	uint64_t max_millis = NumericLimits<uint64_t>::Minimum();
	yyjson_val *max_snapshot = nullptr;

	auto timestamp_millis = Timestamp::GetEpochMs(timestamp);

	yyjson_arr_foreach(snapshots, idx, max, snapshot) {
		auto curr_millis = IcebergUtils::TryGetNumFromObject(snapshot, "timestamp-ms");

		if (curr_millis <= timestamp_millis && curr_millis >= max_millis) {
			max_snapshot = snapshot;
			max_millis = curr_millis;
		}
	}

	return max_snapshot;
}

} // namespace duckdb