#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/file_system.hpp"
#include "iceberg_common.hpp"
#include "iceberg_functions.hpp"
#include "yyjson.hpp"

#include <string>
#include <numeric>

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

	TableFunction parquet_table_function;

	// Data parquet scan
	unique_ptr<FunctionData> parquet_data_bind_data;
	named_parameter_map_t parquet_data_parameter_map;
	vector<Value> parquet_data_parameters;

	// Bind result from data scan
	vector<LogicalType> return_types;
	vector<string> return_names;

	// Delete parquet scan
	unique_ptr<FunctionData> parquet_deletes_bind_data;
	named_parameter_map_t parquet_deletes_parameter_map;
	vector<Value> parquet_deletes_parameters;

	// Bind result from deletes scan (Can be hardcoded instead?)
	vector<LogicalType> delete_return_types;
	vector<string> delete_return_names;
};

static unique_ptr<LogicalOperator> IcebergScanBindReplace(ClientContext &context, const FunctionData *bind_data, BindContext& bind_context, Binder& binder) {
	auto iceberg_bind_data = (IcebergScanBindData*)bind_data;

	auto get_data_bind_index = binder.GenerateTableIndex();

	auto get_data = make_unique<LogicalGet>(get_data_bind_index, iceberg_bind_data->parquet_table_function, std::move(iceberg_bind_data->parquet_data_bind_data), iceberg_bind_data->return_types, iceberg_bind_data->return_names);
	get_data->parameters = iceberg_bind_data->parquet_data_parameters;
	get_data->named_parameters = iceberg_bind_data->parquet_data_parameter_map;
	get_data->input_table_types = {};
	get_data->input_table_names = {};

	// If there's no deletes, we simply return the LogicalGet for the Parquet scan.
	if (!iceberg_bind_data->parquet_deletes_bind_data) {
//		Printer::Print("Scanning only data");
		bind_context.AddTableFunction(get_data_bind_index, "iceberg_scan", iceberg_bind_data->return_names, iceberg_bind_data->return_types, get_data->column_ids, get_data->GetTable());
		return get_data;
	} else {
		bind_context.AddTableFunction(get_data_bind_index, "iceberg_scan_internal_binding_data", iceberg_bind_data->return_names, iceberg_bind_data->return_types, get_data->column_ids, get_data->GetTable());
	}
//	Printer::Print("Scanning data with deletes");

	auto get_deletes_bind_index = binder.GenerateTableIndex();

	auto get_deletes = make_unique<LogicalGet>(get_deletes_bind_index, iceberg_bind_data->parquet_table_function, std::move(iceberg_bind_data->parquet_deletes_bind_data), iceberg_bind_data->delete_return_types, iceberg_bind_data->delete_return_names);
	get_deletes->parameters = iceberg_bind_data->parquet_data_parameters;
	get_deletes->named_parameters = iceberg_bind_data->parquet_data_parameter_map;
	get_deletes->column_ids.resize(iceberg_bind_data->delete_return_types.size());
	std::iota (std::begin(get_deletes->column_ids), std::end(get_deletes->column_ids), 0);
	get_deletes->input_table_types = {};
	get_deletes->input_table_names = {};
	bind_context.AddTableFunction(get_deletes_bind_index, "iceberg_scan_internal_binding_deletes", iceberg_bind_data->delete_return_names, iceberg_bind_data->delete_return_types, get_deletes->column_ids, get_deletes->GetTable());

	// TODO: this is a hacky solution to prevent src/optimizer/remove_unused_columns.cpp from messing with our shit
	// 		 figure out the nice way to do this
	get_data->column_ids.resize(iceberg_bind_data->return_types.size());
	std::iota (std::begin(get_data->column_ids), std::end(get_data->column_ids), 0);

	get_deletes->function.projection_pushdown = false;
	get_data->function.projection_pushdown = false;
	get_deletes->function.filter_pushdown = false;
	get_data->function.filter_pushdown = false;
	get_deletes->function.pushdown_complex_filter = nullptr;
	get_data->function.pushdown_complex_filter = nullptr;

	// Construct the ANTI join for filtering out the delete files
	auto join = make_unique<LogicalComparisonJoin>(JoinType::ANTI);
	join->children.push_back(std::move(get_data));
	join->children.push_back(std::move(get_deletes));
	join->types = iceberg_bind_data->return_types;

	// Construct join conditions for the ANTI join
	JoinCondition cond1;
	cond1.comparison = ExpressionType::COMPARE_NOT_DISTINCT_FROM;

	cond1.left = make_unique<BoundColumnRefExpression>("filename", LogicalType::VARCHAR, ColumnBinding(get_data_bind_index,iceberg_bind_data->return_types.size() - 2));
	cond1.right = make_unique<BoundColumnRefExpression>("filename", LogicalType::VARCHAR, ColumnBinding(get_deletes_bind_index,0));
//	cond1.left = make_unique<BoundReferenceExpression>("filename", LogicalType::VARCHAR, iceberg_bind_data->return_types.size()+1);
//	cond1.right = make_unique<BoundReferenceExpression>("filename", LogicalType::VARCHAR, 0);

	JoinCondition cond2;
	cond2.comparison = ExpressionType::COMPARE_NOT_DISTINCT_FROM;
	cond2.left = make_unique<BoundColumnRefExpression>("file_row_number", LogicalType::BIGINT, ColumnBinding(get_data_bind_index,iceberg_bind_data->return_types.size() - 1));
	cond2.right = make_unique<BoundColumnRefExpression>("file_row_number", LogicalType::BIGINT, ColumnBinding(get_deletes_bind_index,1));
//	cond2.left = make_unique<BoundReferenceExpression>("file_row_number", LogicalType::VARCHAR, iceberg_bind_data->return_types.size()+2);
//	cond2.right = make_unique<BoundReferenceExpression>("file_row_number", LogicalType::VARCHAR, 1);

	join->conditions.push_back(std::move(cond1));
	join->conditions.push_back(std::move(cond2));

	// now add the table function to the bind context so its columns can be bound

	auto join_bind_index = binder.GenerateTableIndex();
	bind_context.AddGenericBinding(join_bind_index, "iceberg_scan",  iceberg_bind_data->return_names, iceberg_bind_data->return_types);

	return join;
}

static unique_ptr<FunctionData> IcebergScanBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	auto ret = make_unique<IcebergScanBindData>();

	FileSystem &fs = FileSystem::GetFileSystem(context);
	auto iceberg_path = input.inputs[0].ToString();

	IcebergSnapshot snapshot_to_scan;
	if (input.inputs.size() > 1) {
		if (input.inputs[1].type() == LogicalType::UBIGINT) {
			snapshot_to_scan = GetSnapshotById(iceberg_path, fs, input.inputs[1].GetValue<uint64_t>());
		} else if (input.inputs[1].type() == LogicalType::TIMESTAMP) {
			snapshot_to_scan = GetSnapshotByTimestamp(iceberg_path, fs, input.inputs[1].GetValue<timestamp_t>());
		} else {
			throw InvalidInputException("Unknown argument type in IcebergScanBind.");
		}
	} else {
		snapshot_to_scan = GetLatestSnapshot(iceberg_path, fs);
	}
	ret->snapshot_id = snapshot_to_scan.sequence_number;

	IcebergTable iceberg_table = GetIcebergTable(iceberg_path, snapshot_to_scan, fs);
	auto data_files = iceberg_table.GetPaths<IcebergManifestContentType::DATA>();
	auto delete_files = iceberg_table.GetPaths<IcebergManifestContentType::DELETE>();

	vector<Value> data_file_values;
	for (auto& data_file: data_files) {
		data_file_values.push_back({GetFullPath(iceberg_path, data_file, fs)});
	}
	vector<Value> delete_file_values;
	for (auto& delete_file: delete_files) {
		delete_file_values.push_back({GetFullPath(iceberg_path, delete_file, fs)});
	}

	// Lookup parquet scan to get actual binding here
	auto& catalog = Catalog::GetSystemCatalog(context);
	auto entry = catalog.GetEntry<TableFunctionCatalogEntry>(context, DEFAULT_SCHEMA, "parquet_scan", true);
	if (!entry) {
		throw InvalidInputException("Iceberg scan could not find parquet table function, which is required. Try loading parquet with 'LOAD parquet;'");
	}

	auto parquet_table_function = entry->functions.GetFunctionByArguments(context, {LogicalType::LIST(LogicalType::VARCHAR)});
	vector<Value> parquet_data_function_inputs = {Value::LIST(data_file_values)};
	named_parameter_map_t parquet_data_named_parameters({{"filename", Value::BOOLEAN(1),}, {"file_row_number", Value::BOOLEAN(1)}});
	TableFunctionBindInput parquet_data_bind_input(parquet_data_function_inputs, parquet_data_named_parameters, input.input_table_types, input.input_table_names, parquet_table_function.function_info.get());

	// Currently we only support parquet files without schema evolution. So we can simply delegate the bind to the
	// parquet bind of the parquet tablefunction that will scan the data files.
	ret->parquet_data_bind_data = parquet_table_function.bind(context, parquet_data_bind_input, return_types, names);

	// Data files
	ret->parquet_table_function = parquet_table_function;
	ret->parquet_data_parameter_map = parquet_data_named_parameters;
	ret->parquet_data_parameters = parquet_data_function_inputs;

	// Delete files
	if (!delete_file_values.empty()) {
		vector<Value> parquet_delete_function_inputs = {Value::LIST(delete_file_values)};
		named_parameter_map_t parquet_delete_named_parameters({});
		TableFunctionBindInput parquet_delete_bind_input(parquet_delete_function_inputs, parquet_delete_named_parameters, input.input_table_types, input.input_table_names, parquet_table_function.function_info.get());
		ret->parquet_deletes_parameter_map = parquet_delete_named_parameters;
		ret->parquet_deletes_parameters = parquet_delete_function_inputs;
		ret->parquet_deletes_bind_data = parquet_table_function.bind(context, parquet_delete_bind_input, ret->delete_return_types, ret->delete_return_names);
	}

	ret->return_types = return_types;
	ret->return_names = names;

	return ret;
}

static void IcebergScanFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto bind_data = (IcebergScanBindData *)data.bind_data;
	if (!bind_data->sent) {
		FlatVector::GetData<uint64_t>(output.data[0])[0] = true;
		output.SetCardinality(1);
		bind_data->sent = true;
		return;
	}

	output.SetCardinality(0);
}

CreateTableFunctionInfo IcebergFunctions::GetIcebergScanFunction() {
	TableFunctionSet function_set("iceberg_scan");

	auto fun = TableFunction({LogicalType::VARCHAR}, IcebergScanFunction, IcebergScanBind,
	              IcebergScanGlobalTableFunctionState::Init);
	fun.bind_replace = IcebergScanBindReplace;
	function_set.AddFunction(fun);

	fun = TableFunction({LogicalType::VARCHAR, LogicalType::UBIGINT}, IcebergScanFunction, IcebergScanBind,
	                         IcebergScanGlobalTableFunctionState::Init);
	fun.bind_replace = IcebergScanBindReplace;
	function_set.AddFunction(fun);

	fun = TableFunction({LogicalType::VARCHAR, LogicalType::TIMESTAMP}, IcebergScanFunction, IcebergScanBind,
	                         IcebergScanGlobalTableFunctionState::Init);
	fun.bind_replace = IcebergScanBindReplace;
	function_set.AddFunction(fun);

	return CreateTableFunctionInfo(function_set);
}

} // namespace duckdb