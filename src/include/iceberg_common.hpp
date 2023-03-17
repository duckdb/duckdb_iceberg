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
#include <iostream>
#include <cstring>

// cpp avro
#include "avro/Compiler.hh"
#include "avro/DataFile.hh"
#include "avro/Decoder.hh"
#include "avro/Encoder.hh"
#include "avro/ValidSchema.hh"
#include "avro/Stream.hh"

// TODO switch these to do full schema reads
#include "avro_codegen/iceberg_manifest_entry_partial.hpp"
#include "avro_codegen/iceberg_manifest_file_partial.hpp"
//#include "avro_codegen/iceberg_manifest_entry_full.hpp"
//#include "avro_codegen/iceberg_manifest_file_full.hpp"

namespace duckdb {

// ---------------------------------------- MISC ---------------------------------------------------
static string FileToString(const string &path, FileSystem &fs, FileOpener* opener) {
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ, FileSystem::DEFAULT_LOCK,
	                          FileSystem::DEFAULT_COMPRESSION, opener);
	auto file_size = handle->GetFileSize();
	string ret_val(file_size, ' ');
	handle->Read((char *)ret_val.c_str(), file_size);
	return ret_val;
}

//! Get the relative path to an iceberg resource
//! it appears that iceberg contain information on their folder name
static string GetFullPath(const string &iceberg_path, const string &relative_file_path, FileSystem &fs) {

	// TODO: figure this out, should the path in the metadata alwoys be absolute and correct?
	return relative_file_path;

//	auto res = relative_file_path.find_first_of(fs.PathSeparator());
//	if (res == string::npos) {
//		throw IOException("Invalid iceberg path found: " + relative_file_path);
//	}
//
//	return fs.JoinPath(iceberg_path, relative_file_path.substr(res + 1));
}


// ---------------------------------------- IJSBJORG ---------------------------------------------------
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

static string MANIFEST_SCHEMA = "{\n"
                                "     \"type\": \"record\",\n"
                                "     \"name\": \"manifest_file\",\n"
                                "     \"fields\" : [\n"
                                "         {\"name\": \"manifest_path\", \"type\": \"string\"},\n"
                                "         {\"name\": \"content\", \"type\" : \"int\"},\n"
                                "         {\"name\": \"sequence_number\", \"type\" : \"long\"}\n"
                                "     ]\n"
                                " }";
//! An entry in the manifest list file (top level AVRO file)
struct IcebergManifest {
	explicit IcebergManifest(const c::manifest_file& schema) {
	    manifest_path = schema.manifest_path;
	    sequence_number = schema.sequence_number;
	    content = (IcebergManifestContentType)schema.content;
	}
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

static string MANIFEST_ENTRY_SCHEMA = "{\n"
                                      "     \"type\": \"record\",\n"
                                      "     \"name\": \"manifest_entry\",\n"
                                      "     \"fields\" : [\n"
                                      "         {\"name\": \"status\", \"type\" : \"int\"},\n"
                                      "         {\"name\": \"data_file\", \"type\": {\n"
                                      "             \"type\": \"record\",\n"
                                      "             \"name\": \"r2\",\n"
                                      "             \"fields\" : [\n"
                                      "                 {\"name\": \"content\", \"type\": \"int\"},\n"
                                      "                 {\"name\": \"file_path\", \"type\": \"string\"},\n"
                                      "                 {\"name\": \"file_format\", \"type\": \"string\"},\n"
                                      "                 {\"name\": \"record_count\", \"type\" : \"long\"}\n"
                                      "           ]}\n"
                                      "         }\n"
                                      "     ]\n"
                                      " }";

//! An entry in a manifest file
struct IcebergManifestEntry {
	explicit IcebergManifestEntry(const c::manifest_entry& schema) {
		status = (IcebergManifestEntryStatusType)schema.status;
		content = (IcebergManifestEntryContentType)schema.data_file.content;
	  	file_path = schema.data_file.file_path;
		file_format = schema.data_file.file_format;
		record_count = schema.data_file.record_count;
	}

	IcebergManifestEntryStatusType status;

	//! ----- Data File Struct ------
	IcebergManifestEntryContentType content;
	string file_path;
	string file_format;
	int64_t record_count;

	void Print() {
		Printer::Print("    -> ManifestEntry = { type: " + IcebergManifestEntryStatusTypeToString(status) +
		               ", content: " + IcebergManifestEntryContentTypeToString(content) + ", file: " + file_path + ", record_count: " + to_string(record_count) + "}");
	}
};

struct IcebergTableEntry {
	IcebergManifest manifest;
	vector<IcebergManifestEntry> manifest_entries;

	void Print() {
		manifest.Print();
		for (auto &manifest_entry : manifest_entries) {
			manifest_entry.Print();
		}
	}
};

struct IcebergTable {
	string path;
	vector<IcebergTableEntry> entries;

	template<IcebergManifestContentType TYPE>
	vector<string> GetPaths() {
		vector<string> ret;
		for (auto &entry : entries) {
			if(entry.manifest.content != TYPE) {
				continue;
			}
			for (auto &manifest_entry : entry.manifest_entries) {
				if (manifest_entry.status == IcebergManifestEntryStatusType::DELETED) {
					continue;
				}
				ret.push_back(manifest_entry.file_path);
			}
		}
		return ret;
	}

	void Print() {
		Printer::Print("Iceberg table (" + path + ")");
		for (auto &entry : entries) {
			entry.Print();
		}
	}
};

// ----------------------------------------- AVRO ----------------------------------------------------

static vector<IcebergManifest> ReadManifestListFile(string path, FileSystem &fs, FileOpener* opener) {
	vector<IcebergManifest> ret;

	// TODO: make streaming
	string file = FileToString(path, fs, opener);
	auto stream = avro::memoryInputStream((unsigned char*)file.c_str(), file.size());
	auto schema = avro::compileJsonSchemaFromString(MANIFEST_SCHEMA);
	avro::DataFileReader<c::manifest_file> dfr(std::move(stream), schema);

	c::manifest_file manifest_list;
	while (dfr.read(manifest_list)) {
		ret.emplace_back(IcebergManifest(manifest_list));
	}

	return ret;
}

static vector<IcebergManifestEntry> ReadManifestEntries(string path, FileSystem &fs, FileOpener* opener) {
	vector<IcebergManifestEntry> ret;

	// TODO: make streaming
	string file = FileToString(path, fs, opener);
	auto stream = avro::memoryInputStream((unsigned char*)file.c_str(), file.size());
	auto schema = avro::compileJsonSchemaFromString(MANIFEST_ENTRY_SCHEMA);
	avro::DataFileReader<c::manifest_entry> dfr(std::move(stream), schema);

	c::manifest_entry manifest_entry;
	while (dfr.read(manifest_entry)) {
		ret.emplace_back(IcebergManifestEntry(manifest_entry));
	}

	return ret;
}

static IcebergTable GetIcebergTable(const string &iceberg_path, IcebergSnapshot &snapshot, FileSystem &fs, FileOpener* opener) {
	IcebergTable ret;
	ret.path = iceberg_path;

	auto manifest_list_full_path = GetFullPath(iceberg_path, snapshot.manifest_list, fs);
	auto manifests = ReadManifestListFile(manifest_list_full_path, fs, opener);

	for (auto &manifest : manifests) {
		auto manifest_entry_full_path = GetFullPath(iceberg_path, manifest.manifest_path, fs);
		auto manifest_paths = ReadManifestEntries(manifest_entry_full_path, fs, opener);

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

static idx_t GetTableVersion(string &path, FileSystem &fs, FileOpener* opener) {
	auto meta_path = fs.JoinPath(path, "metadata");
	auto version_file_path = FileSystem::JoinPath(meta_path, "version-hint.text");
	auto version_file_content = FileToString(version_file_path, fs, opener);

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

static string ReadMetaData(string &path, FileSystem &fs, FileOpener* opener) {
	auto table_version = GetTableVersion(path, fs, opener);

	auto meta_path = fs.JoinPath(path, "metadata");
	auto metadata_file_path = fs.JoinPath(meta_path, "v" + to_string(table_version) + ".metadata.json");

	return FileToString(metadata_file_path, fs, opener);
}

static IcebergSnapshot GetLatestSnapshot(string &path, FileSystem &fs, FileOpener* opener) {
	auto metadata_json = ReadMetaData(path, fs, opener);
	auto doc = yyjson_read(metadata_json.c_str(), metadata_json.size(), 0);
	auto root = yyjson_doc_get_root(doc);
	auto snapshots = yyjson_obj_get(root, "snapshots");
	auto latest_snapshot = FindLatestSnapshotInternal(snapshots);

	if (!latest_snapshot) {
		throw IOException("No snapshots found");
	}

	return ParseSnapShot(latest_snapshot);
}

static IcebergSnapshot GetSnapshotById(string &path, FileSystem &fs, FileOpener* opener, idx_t snapshot_id) {
	auto metadata_json = ReadMetaData(path, fs, opener);
	auto doc = yyjson_read(metadata_json.c_str(), metadata_json.size(), 0);
	auto root = yyjson_doc_get_root(doc);
	auto snapshots = yyjson_obj_get(root, "snapshots");
	auto snapshot = FindSnapshotByIdInternal(snapshots, snapshot_id);

	if (!snapshot) {
		throw IOException("Could not find snapshot with id " + to_string(snapshot_id));
	}

	return ParseSnapShot(snapshot);
}

static IcebergSnapshot GetSnapshotByTimestamp(string &path, FileSystem &fs, FileOpener* opener, timestamp_t timestamp) {
	auto metadata_json = ReadMetaData(path, fs, opener);
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