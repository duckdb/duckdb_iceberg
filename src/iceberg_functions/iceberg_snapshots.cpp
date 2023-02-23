#include "iceberg_functions.hpp"
#include "yyjson.hpp"

#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/file_system.hpp"

#include <string>

namespace duckdb {

struct IcebergSnapshotGlobalTableFunctionState : public GlobalTableFunctionState {
public:
    static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context,TableFunctionInitInput &input) {
      return make_unique<GlobalTableFunctionState>();
    }
};

struct IcebergSnaphotsBindData: public TableFunctionData {
    IcebergSnaphotsBindData() : table_version(0), metadata_doc(nullptr){};
    ~IcebergSnaphotsBindData() {
        if (metadata_doc) {
            yyjson_doc_free(metadata_doc);
        }
    }

    idx_t table_version;
    string metadata_file;
    yyjson_doc *metadata_doc;
    yyjson_arr_iter snapshot_it;
};

struct IceBergSnapshot {
    idx_t snapshot_id;
    idx_t sequence_number;
    idx_t schema_id;
    string manifest_list;
    idx_t timestamp_ms;
};

static IceBergSnapshot ParseSnapShot(yyjson_val* val) {
    IceBergSnapshot ret;
    ret.snapshot_id = yyjson_get_uint(yyjson_obj_get(val, "snapshot-id"));
    ret.sequence_number = yyjson_get_uint(yyjson_obj_get(val, "sequence-number"));
    ret.schema_id = yyjson_get_uint(yyjson_obj_get(val, "schema-id"));
    ret.timestamp_ms = yyjson_get_uint(yyjson_obj_get(val, "timestamp-ms"));
    ret.manifest_list = yyjson_get_str(yyjson_obj_get(val, "manifest-list"));
    return ret;
}

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

    auto meta_path = FileSystem::JoinPath(filename, "metadata");

    // First read the version hint
    auto version_file_path = FileSystem::JoinPath(meta_path, "version-hint.text");
    auto version_file_content = file_to_string(fs, version_file_path);

    try {
        ret->table_version = std::stoll(version_file_content);
    } catch (std::invalid_argument &e) {
        throw IOException("Iceberg version hint file contains invalid value");
    } catch (std::out_of_range &e) {
        throw IOException("Iceberg version hint file contains invalid value");
    }

    // Then read the metadata json file
    auto metadata_file_path =  FileSystem::JoinPath(meta_path, "v" + to_string(ret->table_version) + ".metadata.json");
    ret->metadata_file = file_to_string(fs, metadata_file_path);

    // Ensure we can read the snapshots property here
    ret->metadata_doc = yyjson_read(ret->metadata_file.c_str(), ret->metadata_file.size(), 0);

    auto root = yyjson_doc_get_root(ret->metadata_doc);
    auto snapshots = yyjson_obj_get(root, "snapshots");

    yyjson_arr_iter_init(snapshots, &ret->snapshot_it);

    names.emplace_back("sequence_number");
    return_types.emplace_back(LogicalType::BIGINT);

    names.emplace_back("snapshot-id");
    return_types.emplace_back(LogicalType::BIGINT);

    names.emplace_back("timestamp-ms");
    return_types.emplace_back(LogicalType::BIGINT);

    names.emplace_back("schema-id");
    return_types.emplace_back(LogicalType::BIGINT);

    names.emplace_back("manifest-list");
    return_types.emplace_back(LogicalType::VARCHAR);

    return ret;
}

// Snapshots function
static void IcebergSnapshotsFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
    auto bind_data = (IcebergSnaphotsBindData*)data.bind_data;

    idx_t i = 0;
    while (auto next_snapshot = yyjson_arr_iter_next(&bind_data->snapshot_it)) {
        if (i >= STANDARD_VECTOR_SIZE) {
            break;
        }

        auto snapshot = ParseSnapShot(next_snapshot);

        FlatVector::GetData<uint64_t>(output.data[0])[i] = snapshot.sequence_number;
        FlatVector::GetData<uint64_t>(output.data[1])[i] = snapshot.snapshot_id;
        FlatVector::GetData<uint64_t>(output.data[2])[i] = snapshot.timestamp_ms;
        FlatVector::GetData<uint64_t>(output.data[3])[i] = snapshot.schema_id;

        string_t manifest_string_t = StringVector::AddString(output.data[4],string_t(snapshot.manifest_list));
        FlatVector::GetData<string_t>(output.data[4])[i] = manifest_string_t;

        i++;
    }
    output.SetCardinality(i);
}

CreateTableFunctionInfo IcebergFunctions::GetIcebergSnapshotsFunction() {
    auto function_info = make_shared<TableFunctionInfo>();
    TableFunction table_function("iceberg_snapshots", {LogicalType::VARCHAR}, IcebergSnapshotsFunction, IcebergSnapshotsBind, IcebergSnapshotGlobalTableFunctionState::Init);
    table_function.projection_pushdown = false;
    table_function.function_info = std::move(function_info);

    return CreateTableFunctionInfo(table_function);
}

}