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
#include <string.h>

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
	DELETE = 1
};

//! An entry in the manifest list file (top level AVRO file)
struct IcebergManifest {
	//! Path to the manifest AVRO file
	string manifest_path;
	//! sequence_number when manifest was added to table (0 for Iceberg v1)
	int64_t sequence_number;
	//! either data or deletes
	IcebergManifestContentType content;
};

enum class IcebergManifestEntryStatusType : uint8_t {
	EXISTING = 0,
	ADDED = 1,
	DELETED = 2
};

enum class IcebergManifestEntryContentType : uint8_t {
	DATA = 0,
	POSITION_DELETES = 1,
	EQUALITY_DELETES = 2
};

//! An entry in a manifest file
struct IcebergManifestEntry {
	IcebergManifestEntryStatusType status;

	//! ----- Data File Struct ------
	IcebergManifestEntryContentType content;
	string file_path;
	string file_format;
	int64_t record_count;
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
static string GetFullPath(const string& iceberg_path, const string& relative_file_path, FileSystem& fs) {
	auto res = relative_file_path.find_first_of(fs.PathSeparator());
	if (res == string::npos) {
		throw IOException("Invalid iceberg path found: " + relative_file_path);
	}

	return fs.JoinPath(iceberg_path, relative_file_path.substr(res+1));
}

// ----------------------------------------- AVRO ----------------------------------------------------
static vector<IcebergManifest> ParseManifests(avro_file_reader_t db, avro_schema_t reader_schema)
{
	vector<IcebergManifest> ret;

	avro_value_iface_t  *manifest_class =
	    avro_generic_class_from_schema(reader_schema);

	avro_value_t manifest;
	avro_generic_value_new(manifest_class, &manifest);

	while (!avro_file_reader_read_value(db, &manifest)) {
		const char *path_string = nullptr;
		size_t path_size = 0;
		avro_value_t path_value {nullptr, nullptr};

		if (avro_value_get_by_name(&manifest, "manifest_path", &path_value, nullptr) == 0) {
			avro_value_get_string(&path_value, &path_string, &path_size);
//			std::cout << "Path read: " << string(path_string, path_size-1) << "\n";
		}

		avro_value_t content_value ;
		int content = 0;
		if (avro_value_get_by_name(&manifest, "content", &content_value, nullptr) == 0) {
			avro_value_get_int(&content_value, &content);
		}

//		std::cout << "content_value read: " << to_string(content) << "\n";

		avro_value_t sequence_number_value;
		int64_t sequence_number = 0;
		if (avro_value_get_by_name(&manifest, "sequence_number", &sequence_number_value, nullptr) == 0) {
			avro_value_get_long(&sequence_number_value, &sequence_number);
		}
//		std::cout << "Sequence number read: " << to_string(sequence_number) << "\n";

		ret.push_back({string(path_string, path_size), sequence_number, (IcebergManifestContentType)content});
	}
	avro_value_decref(&manifest);
	avro_value_iface_decref(manifest_class);

	return ret;
}

static vector<IcebergManifestEntry> ParseManifestEntries(avro_file_reader_t db, avro_schema_t reader_schema)
{
	vector<IcebergManifestEntry> ret;

	avro_value_iface_t  *manifest_entry_class =
	    avro_generic_class_from_schema(reader_schema);

	avro_value_t manifest;
	avro_generic_value_new(manifest_entry_class, &manifest);

	while (!avro_file_reader_read_value(db, &manifest)) {
		avro_value_t status_value;
		int status = 0;
		if (avro_value_get_by_name(&manifest, "status", &status_value, nullptr) == 0) {
			avro_value_get_int(&status_value, &status);
		}

		if (avro_record_get())
		//		std::cout << "Sequence number read: " << to_string(sequence_number) << "\n";

		ret.push_back({(IcebergManifestEntryStatusType)status, (IcebergManifestEntryContentType)0, "", "", 0});
	}
	avro_value_decref(&manifest);
	avro_value_iface_decref(manifest_entry_class);

	return ret;
}

static vector<IcebergManifest> ReadManifestListFile(string path, FileSystem &fs) {
	// TODO dumb AVRO C API doesn't expose the right functions to read from memory for some reason, use the disk FS for now
//	auto avro_file = FileToString(path, fs);

	avro_file_reader_t reader;

	auto res = avro_file_reader(path.c_str(), &reader);
	if (res) {
		throw IOException("Failed to open avro file " + string(strerror(res)));
	}

	auto read_schema = avro_file_reader_get_writer_schema(reader);

	return ParseManifests(reader, read_schema);
}

static vector<IcebergManifestEntry> ReadManifestEntry(string path, FileSystem &fs) {
	// TODO dumb AVRO C API doesn't expose the right functions to read from memory for some reason, use the disk FS for now
//	auto avro_file = FileToString(path, fs);

	avro_file_reader_t reader;

	auto res = avro_file_reader(path.c_str(), &reader);
	if (res) {
		throw IOException("Failed to open avro file " + string(strerror(res)));
	}

	auto read_schema = avro_file_reader_get_writer_schema(reader);

	return ParseManifestEntries(reader, read_schema);
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