//===----------------------------------------------------------------------===//
//                         DuckDB
//
// iceberg_metadata.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "yyjson.hpp"
#include "iceberg_types.hpp"

namespace duckdb {

struct IcebergColumnDefinition {
public:
	static IcebergColumnDefinition ParseFromJson(yyjson_val *val);

	LogicalType ToDuckDBType() {
		return type;
	}

	int32_t id;
	string name;
	LogicalType type;
	Value default_value;
	bool required;
};

struct SnapshotParseInfo {
	~SnapshotParseInfo() {
		if (doc) {
			yyjson_doc_free(doc);
		}
	}
	// Ownership of parse data
	yyjson_doc *doc;
	string document;

	//! Parsed info
	yyjson_val *snapshots;
	vector<yyjson_val *> schemas;
	uint64_t iceberg_version;
	uint64_t schema_id;
};

//! An Iceberg snapshot https://iceberg.apache.org/spec/#snapshots
class IcebergSnapshot {
public:
	//! Snapshot metadata
	uint64_t snapshot_id;
	uint64_t sequence_number;
	string manifest_list;
	timestamp_t timestamp_ms;
	idx_t iceberg_format_version;
	uint64_t schema_id;
	vector<IcebergColumnDefinition> schema;

	static IcebergSnapshot GetLatestSnapshot(const string &path, FileSystem &fs);
	static IcebergSnapshot GetSnapshotById(const string &path, FileSystem &fs, idx_t snapshot_id);
	static IcebergSnapshot GetSnapshotByTimestamp(const string &path, FileSystem &fs, timestamp_t timestamp);

	static IcebergSnapshot ParseSnapShot(yyjson_val *snapshot, idx_t iceberg_format_version, idx_t schema_id,
	                                     vector<yyjson_val *> &schemas);
	static string ReadMetaData(const string &path, FileSystem &fs);
	static yyjson_val *GetSnapshots(const string &path, FileSystem &fs);
	static unique_ptr<SnapshotParseInfo> GetParseInfo(yyjson_doc &metadata_json);

protected:
	//! Internal JSON parsing functions
	static string GetTableVersion(const string &path, FileSystem &fs);
	static yyjson_val *FindLatestSnapshotInternal(yyjson_val *snapshots);
	static yyjson_val *FindSnapshotByIdInternal(yyjson_val *snapshots, idx_t target_id);
	static yyjson_val *FindSnapshotByIdTimestampInternal(yyjson_val *snapshots, timestamp_t timestamp);
	static vector<IcebergColumnDefinition> ParseSchema(vector<yyjson_val *> &schemas, idx_t schema_id);
	static unique_ptr<SnapshotParseInfo> GetParseInfo(const string &path, FileSystem &fs);
};

//! Represents the iceberg table at a specific IcebergSnapshot. Corresponds to a single Manifest List.
struct IcebergTable {
public:
	//! Loads all(!) metadata of into IcebergTable object
	static IcebergTable Load(const string &iceberg_path, IcebergSnapshot &snapshot, FileSystem &fs,
	                         bool allow_moved_paths = false);

	//! Returns all paths to be scanned for the IcebergManifestContentType
	template <IcebergManifestContentType TYPE>
	vector<string> GetPaths() {
		vector<string> ret;
		for (auto &entry : entries) {
			if (entry.manifest.content != TYPE) {
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

	//! The snapshot of this table
	IcebergSnapshot snapshot;
	//! The entries (manifests) of this table
	vector<IcebergTableEntry> entries;

protected:
	static vector<IcebergManifest> ReadManifestListFile(const string &path, FileSystem &fs, idx_t iceberg_format_version);
	static vector<IcebergManifestEntry> ReadManifestEntries(const string &path, FileSystem &fs, idx_t iceberg_format_version);
	string path;
};

} // namespace duckdb