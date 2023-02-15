#include "iceberg_functions.hpp"
#include "yyjson.hpp"

#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/file_system.hpp"

#include <string>

namespace duckdb {

struct IcebergSnapshotsInfo : public TableFunctionInfo {
public:
  explicit IcebergSnapshotsInfo() = default;
};

struct IcebergSnapshotGlobalTableFunctionState : public GlobalTableFunctionState {
public:
    static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context,TableFunctionInitInput &input) {
      return make_unique<GlobalTableFunctionState>();
    }
    idx_t MaxThreads(){ return 1; };
};

struct IcebergSnaphotsBindData: public TableFunctionData {
    idx_t table_version;

    string metadata_file;
    yyjson_doc *metadata_doc;
    yyjson_val *metadata_snapshots;
};

static string file_to_string(FileSystem& fs, const string& path) {
    auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
    auto file_size = handle->GetFileSize();
    string ret_val(file_size, ' ');
    handle->Read((char*)ret_val.c_str(), file_size);
    return ret_val;
}

// Bind Snapshots function
static unique_ptr<FunctionData> IcebergSnapshotsBind(ClientContext &context, TableFunctionBindInput &input,
                                                                 vector<LogicalType> &return_types, vector<string> &names) {
    auto ret = make_unique<IcebergSnaphotsBindData>();

    auto filename = input.inputs[0].ToString();
    FileSystem &fs = FileSystem::GetFileSystem(context);

    // First read the version hint
    auto version_file_path = FileSystem::JoinPath(filename, "version-hint.text");
    auto version_file_content = file_to_string(fs, version_file_path);

    try {
        ret->table_version = std::stoll(version_file_content);
    } catch (std::invalid_argument &e) {
        throw IOException("Iceberg version hint file contains invalid value");
    } catch (std::out_of_range &e) {
        throw IOException("Iceberg version hint file contains invalid value");
    }

    // Then read the metadata json file
    auto metadata_file_path =  FileSystem::JoinPath(filename, "v" + to_string(ret->table_version) + ".metadata.json");
    ret->metadata_file = file_to_string(fs, metadata_file_path);


    // Ensure we can read the snapshots property here
    ret->metadata_doc = yyjson_read(ret->metadata_file.c_str(), ret->metadata_file.size(), 0);
    auto root = yyjson_doc_get_root(ret->metadata_doc);
    ret->metadata_snapshots = yyjson_obj_get(root, "snapshots");

    // Free the doc
//    yyjson_doc_free(doc);

    names.emplace_back("sequence_number");
    return_types.emplace_back(LogicalType::BIGINT);

    names.emplace_back("snapshot-id");
    return_types.emplace_back(LogicalType::BIGINT);

    names.emplace_back("timestamp-ms");
    return_types.emplace_back(LogicalType::BIGINT);

    names.emplace_back("manifest-list");
    return_types.emplace_back(LogicalType::VARCHAR);

    names.emplace_back("schema-id");
    return_types.emplace_back(LogicalType::VARCHAR);

    return make_unique<TableFunctionData>();
}

// Snapshots function
static void IcebergSnapshotsFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
    auto& glob_state = (IcebergSnapshotGlobalTableFunctionState&)*data.global_state;

    if (glob_state.csv_file.empty()) {
        auto& opener = *FileOpener::Get(context);
        FileSystem &fs = FileSystem::GetFileSystem(context);
        auto handle = fs.OpenFile(file_name, FileFlags::FILE_FLAGS_READ, FileSystem::DEFAULT_LOCK,
                                  FileSystem::DEFAULT_COMPRESSION, FileSystem::GetFileOpener(context));
    }
}



CreateTableFunctionInfo IcebergFunctions::GetIcebergSnapshotsFunction() {
    TableFunctionSet function_set("iceberg_snapshots");
    auto function_info = make_shared<IcebergSnapshotsInfo>();

    TableFunction table_function({LogicalType::VARCHAR}, IcebergSnapshotsFunction, IcebergSnapshotsBind, IcebergSnapshotGlobalTableFunctionState::Init);
    table_function.projection_pushdown = false;
    table_function.function_info = std::move(function_info);

    return CreateTableFunctionInfo(table_function);
}

}