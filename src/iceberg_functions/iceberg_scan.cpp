#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/file_system.hpp"
#include "iceberg_common.hpp"
#include "iceberg_functions.hpp"
#include "yyjson.hpp"

#include <string>

// NOTE: currently the scan function is a dummy placeholder that returns the sequence number of the snapshot it should
// scan

namespace duckdb {

struct IcebergScanGlobalTableFunctionState : public GlobalTableFunctionState {
public:
	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
		return make_unique<GlobalTableFunctionState>();
	}
};

struct IcebergScanBindData : public TableFunctionData {
	idx_t snapshot_id;
	bool sent = false;
};

static unique_ptr<FunctionData> IcebergScanBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	auto ret = make_unique<IcebergScanBindData>();

	FileSystem &fs = FileSystem::GetFileSystem(context);
	auto iceberg_path = input.inputs[0].ToString();

	IceBergSnapshot snapshot_to_scan;
	if (input.inputs.size() > 1) {
		if (input.inputs[1].type() == LogicalType::UBIGINT) {
			snapshot_to_scan = GetSnapshotById(iceberg_path, fs, input.inputs[1].GetValue<uint64_t>());
		} else if (input.inputs[1].type() == LogicalType::TIMESTAMP) {
			snapshot_to_scan = GetSnapshotByTimestamp(iceberg_path, fs, input.inputs[1].GetValue<timestamp_t>());
		} else {
			throw InternalException("Unknown argument type in IcebergScanBind.");
		}
	} else {
		snapshot_to_scan = GetLatestSnapshot(iceberg_path, fs);
	}

	ret->snapshot_id = snapshot_to_scan.sequence_number;

	names.emplace_back("snapshot to be scanned");
	return_types.emplace_back(LogicalType::UBIGINT);

	return ret;
}

static void IcebergScanFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto bind_data = (IcebergScanBindData *)data.bind_data;
	if (!bind_data->sent) {
		FlatVector::GetData<uint64_t>(output.data[0])[0] = bind_data->snapshot_id;
		output.SetCardinality(1);
		bind_data->sent = true;
		return;
	}

	output.SetCardinality(0);
}

CreateTableFunctionInfo IcebergFunctions::GetIcebergScanFunction() {
	TableFunctionSet function_set("iceberg_scan");

	function_set.AddFunction(TableFunction({LogicalType::VARCHAR}, IcebergScanFunction, IcebergScanBind,
	                                       IcebergScanGlobalTableFunctionState::Init));
	function_set.AddFunction(TableFunction({LogicalType::VARCHAR, LogicalType::UBIGINT}, IcebergScanFunction,
	                                       IcebergScanBind, IcebergScanGlobalTableFunctionState::Init));
	function_set.AddFunction(TableFunction({LogicalType::VARCHAR, LogicalType::TIMESTAMP}, IcebergScanFunction,
	                                       IcebergScanBind, IcebergScanGlobalTableFunctionState::Init));

	return CreateTableFunctionInfo(function_set);
}

} // namespace duckdb