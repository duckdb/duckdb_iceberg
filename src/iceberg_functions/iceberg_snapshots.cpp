#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/file_system.hpp"
#include "iceberg_metadata.hpp"
#include "iceberg_functions.hpp"
#include "yyjson.hpp"

#include <string>

namespace duckdb {

struct IcebergSnapshotGlobalTableFunctionState : public GlobalTableFunctionState {
public:
	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
		return make_uniq<GlobalTableFunctionState>();
	}
};

struct IcebergSnaphotsBindData : public TableFunctionData {
	IcebergSnaphotsBindData() : metadata_doc(nullptr) {};
	~IcebergSnaphotsBindData() {
		if (metadata_doc) {
			yyjson_doc_free(metadata_doc);
		}
	}
	string metadata_file;
	yyjson_doc *metadata_doc;
	yyjson_arr_iter snapshot_it;
};

static unique_ptr<FunctionData> IcebergSnapshotsBind(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<string> &names) {
	auto ret = make_uniq<IcebergSnaphotsBindData>();

	auto filename = input.inputs[0].ToString();

	FileSystem &fs = FileSystem::GetFileSystem(context);
	ret->metadata_file = IcebergSnapshot::ReadMetaData(filename, fs, FileOpener::Get(context));

	// Ensure we can read the snapshots property here
	ret->metadata_doc = yyjson_read(ret->metadata_file.c_str(), ret->metadata_file.size(), 0);

	auto root = yyjson_doc_get_root(ret->metadata_doc);
	auto snapshots = yyjson_obj_get(root, "snapshots");
	yyjson_arr_iter_init(snapshots, &ret->snapshot_it);

	names.emplace_back("sequence_number");
	return_types.emplace_back(LogicalType::UBIGINT);

	names.emplace_back("snapshot_id");
	return_types.emplace_back(LogicalType::UBIGINT);

	names.emplace_back("timestamp_ms");
	return_types.emplace_back(LogicalType::TIMESTAMP);

	names.emplace_back("manifest_list");
	return_types.emplace_back(LogicalType::VARCHAR);

	return ret;
}

// Snapshots function
static void IcebergSnapshotsFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto bind_data = (IcebergSnaphotsBindData *)data.bind_data;

	idx_t i = 0;
	while (auto next_snapshot = yyjson_arr_iter_next(&bind_data->snapshot_it)) {
		if (i >= STANDARD_VECTOR_SIZE) {
			break;
		}
		auto snapshot = IcebergSnapshot::ParseSnapShot(next_snapshot);

		FlatVector::GetData<int64_t>(output.data[0])[i] = snapshot.sequence_number;
		FlatVector::GetData<int64_t>(output.data[1])[i] = snapshot.snapshot_id;
		FlatVector::GetData<timestamp_t>(output.data[2])[i] = snapshot.timestamp_ms;
		string_t manifest_string_t = StringVector::AddString(output.data[3], string_t(snapshot.manifest_list));
		FlatVector::GetData<string_t>(output.data[3])[i] = manifest_string_t;

		i++;
	}
	output.SetCardinality(i);
}

CreateTableFunctionInfo IcebergFunctions::GetIcebergSnapshotsFunction() {
	auto function_info = make_shared<TableFunctionInfo>();
	TableFunction table_function("iceberg_snapshots", {LogicalType::VARCHAR}, IcebergSnapshotsFunction,
	                             IcebergSnapshotsBind, IcebergSnapshotGlobalTableFunctionState::Init);
	table_function.function_info = std::move(function_info);

	return CreateTableFunctionInfo(table_function);
}

} // namespace duckdb