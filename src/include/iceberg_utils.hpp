//===----------------------------------------------------------------------===//
//                         DuckDB
//
// iceberg_utils.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/printer.hpp"
#include "iceberg_types.hpp"
#include "yyjson.hpp"

namespace duckdb {

//! An entry in the metadata.json snapshots field
class IcebergSnapshot {
public:
	//! Snapshot metadata
	uint64_t snapshot_id;
	uint64_t sequence_number;
	uint64_t schema_id;
	string manifest_list;
	timestamp_t timestamp_ms;

	static IcebergSnapshot GetLatestSnapshot(string &path, FileSystem &fs);
	static IcebergSnapshot GetSnapshotById(string &path, FileSystem &fs, idx_t snapshot_id);
	static IcebergSnapshot GetSnapshotByTimestamp(string &path, FileSystem &fs, timestamp_t timestamp);

	static IcebergSnapshot ParseSnapShot(yyjson_val *snapshot);
	static string ReadMetaData(string &path, FileSystem &fs);

protected:
	//! Internal JSON parsing functions
	static idx_t GetTableVersion(string &path, FileSystem &fs);
	static yyjson_val *FindLatestSnapshotInternal(yyjson_val *snapshots);
	static yyjson_val *FindSnapshotByIdInternal(yyjson_val *snapshots, idx_t target_id);
	static yyjson_val *FindSnapshotByIdTimestampInternal(yyjson_val *snapshots, timestamp_t timestamp);
};

//! Main class holding all metadata for an iceberg table at a specific snapshot
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

protected:
	static vector<IcebergManifest> ReadManifestListFile(string path, FileSystem &fs);
	static vector<IcebergManifestEntry> ReadManifestEntries(string path, FileSystem &fs);

	string path;
	vector<IcebergTableEntry> entries;
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