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

using namespace duckdb_yyjson;

namespace duckdb {

static string VERSION_GUESSING_CONFIG_VARIABLE = "unsafe_enable_version_guessing";

// When this is provided (and unsafe_enable_version_guessing is true)
// we first look for DEFAULT_VERSION_HINT_FILE, if it doesn't exist we 
// then search for versions matching the DEFAULT_TABLE_VERSION_FORMAT
// We take the lexographically "greatest" one as the latest version
// Note that this will voliate ACID constraints in some situations.
static string UNKNOWN_TABLE_VERSION = "?";

// First arg is version string, arg is either empty or ".gz" if gzip
// Allows for both "v###.gz.metadata.json" and "###.metadata.json" styles
static string DEFAULT_TABLE_VERSION_FORMAT = "v%s%s.metadata.json,%s%s.metadata.json";

// This isn't explicitly in the standard, but is a commonly used technique
static string DEFAULT_VERSION_HINT_FILE = "version-hint.text";

// By default we will use the unknown version behavior mentioned above
static string DEFAULT_TABLE_VERSION = UNKNOWN_TABLE_VERSION;

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
	string metadata_compression_codec = "none";

	static IcebergSnapshot GetLatestSnapshot(const string &path, FileSystem &fs, string metadata_compression_codec, bool skip_schema_inference);
	static IcebergSnapshot GetSnapshotById(const string &path, FileSystem &fs, idx_t snapshot_id, string metadata_compression_codec, bool skip_schema_inference);
	static IcebergSnapshot GetSnapshotByTimestamp(const string &path, FileSystem &fs, timestamp_t timestamp, string metadata_compression_codec, bool skip_schema_inference);

	static IcebergSnapshot ParseSnapShot(yyjson_val *snapshot, idx_t iceberg_format_version, idx_t schema_id,
	                                     vector<yyjson_val *> &schemas, string metadata_compression_codec, bool skip_schema_inference);
	static string GetMetaDataPath(ClientContext &context, const string &path, FileSystem &fs, string metadata_compression_codec, string table_version, string version_format);
	static string ReadMetaData(const string &path, FileSystem &fs, string metadata_compression_codec);
	static yyjson_val *GetSnapshots(const string &path, FileSystem &fs, string GetSnapshotByTimestamp);
	static unique_ptr<SnapshotParseInfo> GetParseInfo(yyjson_doc &metadata_json);

protected:
	//! Version extraction and identification
	static bool UnsafeVersionGuessingEnabled(ClientContext &context);
	static string GetTableVersionFromHint(const string &path, FileSystem &fs, string version_format);
	static string GuessTableVersion(const string &meta_path, FileSystem &fs, string &table_version, string &metadata_compression_codec, string &version_format);
	static string PickTableVersion(vector<string> &found_metadata, string &version_pattern, string &glob);
	//! Internal JSON parsing functions
	static yyjson_val *FindLatestSnapshotInternal(yyjson_val *snapshots);
	static yyjson_val *FindSnapshotByIdInternal(yyjson_val *snapshots, idx_t target_id);
	static yyjson_val *FindSnapshotByIdTimestampInternal(yyjson_val *snapshots, timestamp_t timestamp);
	static vector<IcebergColumnDefinition> ParseSchema(vector<yyjson_val *> &schemas, idx_t schema_id);
	static unique_ptr<SnapshotParseInfo> GetParseInfo(const string &path, FileSystem &fs, string metadata_compression_codec);
};

//! Represents the iceberg table at a specific IcebergSnapshot. Corresponds to a single Manifest List.
struct IcebergTable {
public:
	//! Loads all(!) metadata of into IcebergTable object
	static IcebergTable Load(const string &iceberg_path, IcebergSnapshot &snapshot, FileSystem &fs,
	                         bool allow_moved_paths = false, string metadata_compression_codec = "none");

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
