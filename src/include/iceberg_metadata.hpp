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

//! An Iceberg snapshot https://iceberg.apache.org/spec/#snapshots
class IcebergSnapshot {
public:
	//! Snapshot metadata
	uint64_t snapshot_id;
	uint64_t sequence_number;
	string manifest_list;
	timestamp_t timestamp_ms;
	idx_t iceberg_format_version;

	static IcebergSnapshot GetLatestSnapshot(string &path, FileSystem &fs);
	static IcebergSnapshot GetSnapshotById(string &path, FileSystem &fs, idx_t snapshot_id);
	static IcebergSnapshot GetSnapshotByTimestamp(string &path, FileSystem &fs, timestamp_t timestamp);

	static IcebergSnapshot ParseSnapShot(yyjson_val *snapshot, idx_t iceberg_format_version);
	static string ReadMetaData(string &path, FileSystem &fs);

protected:
	//! Internal JSON parsing functions
	static idx_t GetTableVersion(string &path, FileSystem &fs);
	static yyjson_val *FindLatestSnapshotInternal(yyjson_val *snapshots);
	static yyjson_val *FindSnapshotByIdInternal(yyjson_val *snapshots, idx_t target_id);
	static yyjson_val *FindSnapshotByIdTimestampInternal(yyjson_val *snapshots, timestamp_t timestamp);
	static case_insensitive_map_t<LogicalType> ParseSchema(yyjson_val *schemas);
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
	static vector<IcebergManifest> ReadManifestListFile(string path, FileSystem &fs, idx_t iceberg_format_version);
	static vector<IcebergManifestEntry> ReadManifestEntries(string path, FileSystem &fs, idx_t iceberg_format_version);
	string path;
};

class IcebergUtils {
public:
	//! Downloads a file fully into a string
	static string FileToString(const string &path, FileSystem &fs);

	//! Somewhat hacky function that allows relative paths in iceberg tables to be resolved,
	//! used for the allow_moved_paths debug option which allows us to test with iceberg tables that
	//! were moved without their paths updated
	static string GetFullPath(const string &iceberg_path, const string &relative_file_path, FileSystem &fs);

	//! YYJSON utility functions
	static uint64_t TryGetNumFromObject(yyjson_val *obj, string field);
	static string TryGetStrFromObject(yyjson_val *obj, string field);
};

} // namespace duckdb