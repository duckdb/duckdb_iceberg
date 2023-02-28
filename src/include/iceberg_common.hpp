//===----------------------------------------------------------------------===//
//                         DuckDB
//
// iceberg_common.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/common/printer.hpp"
#include "yyjson.hpp"
#include <avro.h>
#include <iostream>
#include <cstring>

namespace duckdb {

//! An entry in the metadata.json snapshots field
struct IcebergSnapshot {
	uint64_t snapshot_id;
	uint64_t sequence_number;
	uint64_t schema_id;
	string manifest_list;
	timestamp_t timestamp_ms;
};

enum class IcebergManifestContentType : uint8_t {
	DATA = 0,
	DELETE = 1,
};

static string IcebergManifestContentTypeToString(IcebergManifestContentType type) {
	switch (type) {
	case IcebergManifestContentType::DATA:
		return "DATA";
	case IcebergManifestContentType::DELETE:
		return "DELETE";
	}
}

enum class IcebergManifestEntryStatusType : uint8_t { EXISTING = 0, ADDED = 1, DELETED = 2 };

static string IcebergManifestEntryStatusTypeToString(IcebergManifestEntryStatusType type) {
	switch (type) {
	case IcebergManifestEntryStatusType::EXISTING:
		return "EXISTING";
	case IcebergManifestEntryStatusType::ADDED:
		return "ADDED";
	case IcebergManifestEntryStatusType::DELETED:
		return "DELETED";
	}
}

enum class IcebergManifestEntryContentType : uint8_t { DATA = 0, POSITION_DELETES = 1, EQUALITY_DELETES = 2 };

static string IcebergManifestEntryContentTypeToString(IcebergManifestEntryContentType type) {
	switch (type) {
	case IcebergManifestEntryContentType::DATA:
		return "EXISTING";
	case IcebergManifestEntryContentType::POSITION_DELETES:
		return "POSITION_DELETES";
	case IcebergManifestEntryContentType::EQUALITY_DELETES:
		return "EQUALITY_DELETES";
	}
}

//! An entry in the manifest list file (top level AVRO file)
struct IcebergManifest {
	//! Path to the manifest AVRO file
	string manifest_path;
	//! sequence_number when manifest was added to table (0 for Iceberg v1)
	int64_t sequence_number;
	//! either data or deletes
	IcebergManifestContentType content;

	void Print() {
		Printer::Print("  - Manifest = { content: " + IcebergManifestContentTypeToString(content) +
		               ", path: " + manifest_path + "}");
	}
};

//! An entry in a manifest file
struct IcebergManifestEntry {
	IcebergManifestEntryStatusType status;

	//! ----- Data File Struct ------
	IcebergManifestEntryContentType content;
	string file_path;
	string file_format;
	int64_t record_count;

	void Print() {
		Printer::Print("    -> ManifestEntry = { type: " + IcebergManifestEntryStatusTypeToString(status) +
		               ", content: " + IcebergManifestEntryContentTypeToString(content) + ", file: " + file_path + "." +
		               file_format + ", record_count: " + to_string(record_count) + "}");
	}
};

struct IcebergTableEntry {
	IcebergManifest manifest;
	vector<IcebergManifestEntry> entries;

	void Print() {
		manifest.Print();
		for (auto &entry : entries) {
			entry.Print();
		}
	}
};

struct IcebergTable {
	string path;
	vector<IcebergTableEntry> entries;

	void Print() {
		Printer::Print("Iceberg table (" + path + ")");
		for (auto &entry : entries) {
			entry.Print();
		}
	}
};

// ---------------------------------------- MISC ---------------------------------------------------
static string FileToString(const string &path, FileSystem &fs) {
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
	auto file_size = handle->GetFileSize();
	string ret_val(file_size, ' ');
	handle->Read((char *)ret_val.c_str(), file_size);
	return ret_val;
}

//! Get the relative path to an iceberg resource
//! it appears that iceberg contain information on their folder name
static string GetFullPath(const string &iceberg_path, const string &relative_file_path, FileSystem &fs) {
	auto res = relative_file_path.find_first_of(fs.PathSeparator());
	if (res == string::npos) {
		throw IOException("Invalid iceberg path found: " + relative_file_path);
	}

	return fs.JoinPath(iceberg_path, relative_file_path.substr(res + 1));
}

// ----------------------------------------- AVRO ----------------------------------------------------
static int AvroReadInt(avro_value_t *val, const char *name, const string &filename) {
	avro_value_t ret_value;
	int ret = 0;
	if (avro_value_get_by_name(val, name, &ret_value, nullptr) == 0) {
		avro_value_get_int(&ret_value, &ret);
	} else {
		throw IOException("Failed to read field '" + string(name) + "' from " + filename);
	}
	return ret;
}

static int64_t AvroReadLong(avro_value_t *val, const char *name, const string &filename) {
	avro_value_t ret_value;
	int64_t ret = 0;
	if (avro_value_get_by_name(val, name, &ret_value, nullptr) == 0) {
		avro_value_get_long(&ret_value, &ret);
	} else {
		throw IOException("Failed to read field '" + string(name) + "' from " + filename);
	}
	return ret;
}

static string AvroReadString(avro_value_t *val, const char *name, const string &filename) {
	const char *str = nullptr;
	size_t str_size = 0;
	avro_value_t str_value;
	if (avro_value_get_by_name(val, name, &str_value, nullptr) == 0) {
		avro_value_get_string(&str_value, &str, &str_size);
	} else {
		throw IOException("Failed to read field '" + string(name) + "' from " + filename);
	}
	return {str, str_size - 1};
}

static vector<IcebergManifest> ParseManifests(avro_file_reader_t db, avro_schema_t reader_schema, string &filename) {
	vector<IcebergManifest> ret;

	avro_value_iface_t *manifest_class = avro_generic_class_from_schema(reader_schema);
	avro_value_t manifest;
	avro_generic_value_new(manifest_class, &manifest);

	while (!avro_file_reader_read_value(db, &manifest)) {
		auto path = AvroReadString(&manifest, "manifest_path", filename);
		auto content = AvroReadInt(&manifest, "content", filename);
		auto sequence_number = AvroReadLong(&manifest, "sequence_number", filename);

		ret.push_back({path, sequence_number, (IcebergManifestContentType)content});
	}
	avro_value_decref(&manifest);
	avro_value_iface_decref(manifest_class);
	return ret;
}

static vector<IcebergManifestEntry> ParseManifestEntries(avro_file_reader_t db, avro_schema_t reader_schema,
                                                         const string &filename) {
	vector<IcebergManifestEntry> ret;

	avro_value_iface_t *manifest_entry_class = avro_generic_class_from_schema(reader_schema);

	avro_value_t manifest;
	avro_generic_value_new(manifest_entry_class, &manifest);

	while (!avro_file_reader_read_value(db, &manifest)) {
		auto status = AvroReadInt(&manifest, "status", filename);
		avro_value_t data_file_record_value;
		if (avro_value_get_by_name(&manifest, "data_file", &data_file_record_value, nullptr) == 0) {
			auto content = AvroReadInt(&data_file_record_value, "content", filename);
			auto path = AvroReadString(&data_file_record_value, "file_path", filename);
			auto file_format = AvroReadString(&data_file_record_value, "file_path", filename);
			auto record_count = AvroReadLong(&data_file_record_value, "record_count", filename);
			ret.push_back({(IcebergManifestEntryStatusType)status, (IcebergManifestEntryContentType)content, path,
			               file_format, record_count});
		} else {
			throw IOException("Could not read 'data_file' record from " + filename); // TODO filename
		}
	}
	avro_value_decref(&manifest);
	avro_value_iface_decref(manifest_entry_class);

	return ret;
}

static void OpenAvroFile(string &path, FileSystem &fs, avro_file_reader_t *reader, avro_schema_t *schema) {
	// TODO: AVRO C API doesn't expose the right functions to read from memory for some reason, use the disk FS for now
	//       we will need to write our own code to support duckdb's file system here
	auto res = avro_file_reader(path.c_str(), reader);
	if (res) {
		throw IOException("Failed to open avro file '" + path + "' with error: " + string(strerror(res)));
	}
	*schema = avro_file_reader_get_writer_schema(*reader);
}

static vector<IcebergManifest> ReadManifestListFile(string path, FileSystem &fs) {
	avro_file_reader_t reader;
	avro_schema_t schema;
	OpenAvroFile(path, fs, &reader, &schema);
	return ParseManifests(reader, schema, path);
}

static vector<IcebergManifestEntry> ReadManifestEntries(string path, FileSystem &fs) {
	avro_file_reader_t reader;
	avro_schema_t schema;
	OpenAvroFile(path, fs, &reader, &schema);
	return ParseManifestEntries(reader, schema, path);
}

static IcebergTable GetIcebergTable(const string &iceberg_path, IcebergSnapshot &snapshot, FileSystem &fs) {
	IcebergTable ret;
	ret.path = iceberg_path;

	auto manifest_list_full_path = GetFullPath(iceberg_path, snapshot.manifest_list, fs);
	auto manifests = ReadManifestListFile(manifest_list_full_path, fs);

	for (auto &manifest : manifests) {
		auto manifest_entry_full_path = GetFullPath(iceberg_path, manifest.manifest_path, fs);
		auto manifest_paths = ReadManifestEntries(manifest_entry_full_path, fs);

		ret.entries.push_back({std::move(manifest), std::move(manifest_paths)});
	}

	return ret;
}

// ---------------------------------------- YYJSON ---------------------------------------------------
static uint64_t TryGetNumFromObject(yyjson_val *obj, string field) {
	auto val = yyjson_obj_getn(obj, field.c_str(), field.size());
	if (!val || yyjson_get_tag(val) != YYJSON_TYPE_NUM) {
		throw IOException("Invalid field found while parsing field: " + field);
	}
	return yyjson_get_uint(val);
}

static string TryGetStrFromObject(yyjson_val *obj, string field) {
	auto val = yyjson_obj_getn(obj, field.c_str(), field.size());
	if (!val || yyjson_get_tag(val) != YYJSON_TYPE_STR) {
		throw IOException("Invalid field found while parsing field: " + field);
	}
	return yyjson_get_str(val);
}

static IcebergSnapshot ParseSnapShot(yyjson_val *snapshot) {
	IcebergSnapshot ret;

	auto snapshot_tag = yyjson_get_tag(snapshot);
	if (snapshot_tag != YYJSON_TYPE_OBJ) {
		throw IOException("Invalid snapshot field found parsing iceberg metadata.json");
	}

	ret.snapshot_id = TryGetNumFromObject(snapshot, "snapshot-id");
	ret.sequence_number = TryGetNumFromObject(snapshot, "sequence-number");
	ret.timestamp_ms = Timestamp::FromEpochMs(TryGetNumFromObject(snapshot, "timestamp-ms"));
	ret.schema_id = TryGetNumFromObject(snapshot, "schema-id");
	ret.manifest_list = TryGetStrFromObject(snapshot, "manifest-list");

	return ret;
}

static idx_t GetTableVersion(string &path, FileSystem &fs) {
	auto meta_path = fs.JoinPath(path, "metadata");
	auto version_file_path = FileSystem::JoinPath(meta_path, "version-hint.text");
	auto version_file_content = FileToString(version_file_path, fs);

	try {
		return std::stoll(version_file_content);
	} catch (std::invalid_argument &e) {
		throw IOException("Iceberg version hint file contains invalid value");
	} catch (std::out_of_range &e) {
		throw IOException("Iceberg version hint file contains invalid value");
	}
}

static yyjson_val *FindLatestSnapshotInternal(yyjson_val *snapshots) {
	size_t idx, max;
	yyjson_val *snapshot;

	uint64_t max_timestamp = NumericLimits<uint64_t>::Minimum();
	yyjson_val *max_snapshot = nullptr;

	yyjson_arr_foreach(snapshots, idx, max, snapshot) {

		auto timestamp = TryGetNumFromObject(snapshot, "timestamp-ms");
		if (timestamp >= max_timestamp) {
			max_timestamp = timestamp;
			max_snapshot = snapshot;
		}
	}

	return max_snapshot;
}

static yyjson_val *FindSnapshotByIdInternal(yyjson_val *snapshots, idx_t target_id) {
	size_t idx, max;
	yyjson_val *snapshot;

	yyjson_arr_foreach(snapshots, idx, max, snapshot) {

		auto snapshot_id = TryGetNumFromObject(snapshot, "snapshot-id");

		if (snapshot_id == target_id) {
			return snapshot;
		}
	}

	return nullptr;
}

static yyjson_val *FindSnapshotByIdTimestampInternal(yyjson_val *snapshots, timestamp_t timestamp) {
	size_t idx, max;
	yyjson_val *snapshot;

	uint64_t max_millis = NumericLimits<uint64_t>::Minimum();
	yyjson_val *max_snapshot = nullptr;

	auto timestamp_millis = Timestamp::GetEpochMs(timestamp);

	yyjson_arr_foreach(snapshots, idx, max, snapshot) {
		auto curr_millis = TryGetNumFromObject(snapshot, "timestamp-ms");

		if (curr_millis <= timestamp_millis && curr_millis >= max_millis) {
			max_snapshot = snapshot;
			max_millis = curr_millis;
		}
	}

	return max_snapshot;
}

static string ReadMetaData(string &path, FileSystem &fs) {
	auto table_version = GetTableVersion(path, fs);

	auto meta_path = fs.JoinPath(path, "metadata");
	auto metadata_file_path = fs.JoinPath(meta_path, "v" + to_string(table_version) + ".metadata.json");

	return FileToString(metadata_file_path, fs);
}

static IcebergSnapshot GetLatestSnapshot(string &path, FileSystem &fs) {
	auto metadata_json = ReadMetaData(path, fs);
	auto doc = yyjson_read(metadata_json.c_str(), metadata_json.size(), 0);
	auto root = yyjson_doc_get_root(doc);
	auto snapshots = yyjson_obj_get(root, "snapshots");
	auto latest_snapshot = FindLatestSnapshotInternal(snapshots);

	if (!latest_snapshot) {
		throw IOException("No snapshots found");
	}

	return ParseSnapShot(latest_snapshot);
}

static IcebergSnapshot GetSnapshotById(string &path, FileSystem &fs, idx_t snapshot_id) {
	auto metadata_json = ReadMetaData(path, fs);
	auto doc = yyjson_read(metadata_json.c_str(), metadata_json.size(), 0);
	auto root = yyjson_doc_get_root(doc);
	auto snapshots = yyjson_obj_get(root, "snapshots");
	auto snapshot = FindSnapshotByIdInternal(snapshots, snapshot_id);

	if (!snapshot) {
		throw IOException("Could not find snapshot with id " + to_string(snapshot_id));
	}

	return ParseSnapShot(snapshot);
}

static IcebergSnapshot GetSnapshotByTimestamp(string &path, FileSystem &fs, timestamp_t timestamp) {
	auto metadata_json = ReadMetaData(path, fs);
	auto doc = yyjson_read(metadata_json.c_str(), metadata_json.size(), 0);
	auto root = yyjson_doc_get_root(doc);
	auto snapshots = yyjson_obj_get(root, "snapshots");
	auto snapshot = FindSnapshotByIdTimestampInternal(snapshots, timestamp);

	if (!snapshot) {
		throw IOException("Could not find latest snapshots for timestamp " + Timestamp::ToString(timestamp));
	}

	return ParseSnapShot(snapshot);
}

} // namespace duckdb