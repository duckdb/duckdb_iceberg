#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/common/enums/joinref_type.hpp"
#include "duckdb/common/enums/tableref_type.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
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

static unique_ptr<TableRef> IcebergScanBindReplace(ClientContext &context, TableFunctionBindInput &input) {
	// return a TableRef that contains the scans for the
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
			throw InvalidInputException("Unknown argument type in IcebergScanBindReplace.");
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

	// No deletes, just return a TableFunctionRef for a parquet scan of the data files
	if (delete_files.empty()) {
		auto table_function_ref_data = make_unique<TableFunctionRef>();
		table_function_ref_data->alias = "iceberg_scan_data";
		vector<unique_ptr<ParsedExpression>> left_children;
		left_children.push_back(make_unique<ConstantExpression>(Value::LIST(data_file_values)));
		left_children.push_back(make_unique<ComparisonExpression>(ExpressionType::COMPARE_EQUAL, make_unique<ColumnRefExpression>("filename"), make_unique<ConstantExpression>(Value(1))));
		left_children.push_back(make_unique<ComparisonExpression>(ExpressionType::COMPARE_EQUAL, make_unique<ColumnRefExpression>("file_row_number"), make_unique<ConstantExpression>(Value(1))));
		table_function_ref_data->function = make_unique<FunctionExpression>("parquet_scan", std::move(left_children));
		return table_function_ref_data;
	}

	// Join
	auto join_node = make_unique<JoinRef>(JoinRefType::REGULAR);
	join_node->type = JoinType::ANTI;
	join_node->condition = make_unique<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND,
	                                                          make_unique<ComparisonExpression>(ExpressionType::COMPARE_NOT_DISTINCT_FROM, make_unique<ColumnRefExpression>("filename", "iceberg_scan_data"), make_unique<ColumnRefExpression>("file_path", "iceberg_scan_deletes")),
	                                                          make_unique<ComparisonExpression>(ExpressionType::COMPARE_NOT_DISTINCT_FROM, make_unique<ColumnRefExpression>("file_row_number", "iceberg_scan_data"), make_unique<ColumnRefExpression>("pos", "iceberg_scan_deletes")));

	// LHS: data
	auto table_function_ref_data = make_unique<TableFunctionRef>();
	table_function_ref_data->alias = "iceberg_scan_data";
	vector<unique_ptr<ParsedExpression>> left_children;
	left_children.push_back(make_unique<ConstantExpression>(Value::LIST(data_file_values)));
	left_children.push_back(make_unique<ComparisonExpression>(ExpressionType::COMPARE_EQUAL, make_unique<ColumnRefExpression>("filename"), make_unique<ConstantExpression>(Value(1))));
	left_children.push_back(make_unique<ComparisonExpression>(ExpressionType::COMPARE_EQUAL, make_unique<ColumnRefExpression>("file_row_number"), make_unique<ConstantExpression>(Value(1))));
	table_function_ref_data->function = make_unique<FunctionExpression>("parquet_scan", std::move(left_children));
	join_node->left = std::move(table_function_ref_data);

	// RHS: deletes
	auto table_function_ref_deletes = make_unique<TableFunctionRef>();
	table_function_ref_deletes->alias = "iceberg_scan_deletes";
	vector<unique_ptr<ParsedExpression>> right_children;
	right_children.push_back(make_unique<ConstantExpression>(Value::LIST(delete_file_values)));
	table_function_ref_deletes->function = make_unique<FunctionExpression>("parquet_scan", std::move(right_children));
	join_node->right = std::move(table_function_ref_deletes);

	// Wrap the join in a select, exclude the filename and file_row_number cols
	auto select_statement = make_unique<SelectStatement>();

	// Construct Select node
	auto select_node = make_unique<SelectNode>();
	select_node->from_table = std::move(join_node);
	auto select_expr = make_unique<StarExpression>();
	select_expr->exclude_list = {"filename", "file_row_number"};
	vector<unique_ptr<ParsedExpression>> select_exprs;
	select_exprs.push_back(std::move(select_expr));
	select_node->select_list = std::move(select_exprs);
	select_statement->node = std::move(select_node);

	return make_unique<SubqueryRef>(std::move(select_statement), "iceberg_scan");
}

static unique_ptr<FunctionData> IcebergScanBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	return nullptr;
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