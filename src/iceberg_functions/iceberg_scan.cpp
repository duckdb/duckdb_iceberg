#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/common/enums/joinref_type.hpp"
#include "duckdb/common/enums/tableref_type.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/query_node/recursive_cte_node.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/tableref/emptytableref.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/file_system.hpp"
#include "iceberg_metadata.hpp"
#include "iceberg_utils.hpp"
#include "iceberg_functions.hpp"
#include "yyjson.hpp"

#include <string>
#include <numeric>

namespace duckdb {

struct IcebergScanGlobalTableFunctionState : public GlobalTableFunctionState {
public:
	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
		return make_uniq<GlobalTableFunctionState>();
	}
};

static unique_ptr<ParsedExpression> GetFilenameExpr(unique_ptr<ColumnRefExpression> colref_expr) {
	vector<unique_ptr<ParsedExpression>> split_children;
	split_children.push_back(std::move(colref_expr));
	split_children.push_back(make_uniq<ConstantExpression>(Value("/")));
	auto data_split = make_uniq<FunctionExpression>("string_split", std::move(split_children));

	vector<unique_ptr<ParsedExpression>> list_extract_children;
	list_extract_children.push_back(std::move(data_split));
	list_extract_children.push_back(make_uniq<ConstantExpression>(Value(-1)));
	auto list_extract_expr = make_uniq<FunctionExpression>("list_extract", std::move(list_extract_children));

	return std::move(list_extract_expr);
}

static unique_ptr<ParsedExpression> GetFilenameMatchExpr() {
	auto data_colref_expr = make_uniq<ColumnRefExpression>("filename", "iceberg_scan_data");
	auto delete_colref_expr = make_uniq<ColumnRefExpression>("file_path", "iceberg_scan_deletes");

	auto data_filename_expr = GetFilenameExpr(std::move(data_colref_expr));
	auto delete_filename_expr = GetFilenameExpr(std::move(delete_colref_expr));

	return make_uniq<ComparisonExpression>(ExpressionType::COMPARE_NOT_DISTINCT_FROM, std::move(data_filename_expr),
	                                       std::move(delete_filename_expr));
};

//! Uses recursive unnest on list of structs to return a table with all data and delete files
//! TODO: refactor, probably.
static unique_ptr<TableRef> MakeListFilesExpression(vector<Value> &data_file_values,
                                                    vector<Value> &delete_file_values) {
	vector<Value> structs;
	for (const auto &file : data_file_values) {
		child_list_t<Value> child;
		child.emplace_back(make_pair("file", file));
		child.emplace_back(make_pair("type", Value("data")));
		structs.push_back(Value::STRUCT(child));
	}
	for (const auto &file : delete_file_values) {
		child_list_t<Value> child;
		child.emplace_back(make_pair("file", file));
		child.emplace_back(make_pair("type", Value("delete")));
		structs.push_back(Value::STRUCT(child));
	}

	// Unnest
	vector<unique_ptr<ParsedExpression>> unnest_children;
	unnest_children.push_back(make_uniq<ConstantExpression>(Value::LIST(structs)));
	auto recursive_named_param = make_uniq<ConstantExpression>(Value::BOOLEAN(true));
	recursive_named_param->alias = "recursive";
	unnest_children.push_back(std::move(recursive_named_param));

	// Select node
	auto select_node = make_uniq<SelectNode>();
	vector<unique_ptr<ParsedExpression>> select_exprs;
	select_exprs.emplace_back(make_uniq<FunctionExpression>("unnest", std::move(unnest_children)));
	select_node->select_list = std::move(select_exprs);
	select_node->from_table = make_uniq<EmptyTableRef>();

	// Select statement
	auto select_statement = make_uniq<SelectStatement>();
	select_statement->node = std::move(select_node);
	return make_uniq<SubqueryRef>(std::move(select_statement), "iceberg_scan");
}

// Create the param for passing the iceberg schema to the parquet reader as a DuckDB map
static Value GetParquetSchemaParam(vector<IcebergColumnDefinition> &schema) {
	vector<Value> map_entries;

	for (auto &schema_entry : schema) {
		child_list_t<Value> map_value_children;
		map_value_children.push_back(make_pair("name", Value(schema_entry.name)));
		map_value_children.push_back(make_pair("type", Value(schema_entry.type.ToString())));
		map_value_children.push_back(make_pair("default_value", schema_entry.default_value));
		auto map_value = Value::STRUCT(map_value_children);

		child_list_t<Value> map_entry_children;
		map_entry_children.push_back(make_pair("key", schema_entry.id));
		map_entry_children.push_back(make_pair("values", map_value));
		auto map_entry = Value::STRUCT(map_entry_children);

		map_entries.push_back(map_entry);
	}

	auto param_type =
	    LogicalType::STRUCT({{"key", LogicalType::INTEGER},
	                         {"value", LogicalType::STRUCT({{{"name", LogicalType::VARCHAR},
	                                                         {"type", LogicalType::VARCHAR},
	                                                         {"default_value", LogicalType::VARCHAR}}})}});
	auto ret = Value::MAP(param_type, map_entries);
	return ret;
}

//! Build the Parquet Scan expression for the files we need to scan
static unique_ptr<TableRef> MakeScanExpression(vector<Value> &data_file_values, vector<Value> &delete_file_values,
                                               vector<IcebergColumnDefinition> &schema, bool allow_moved_paths,
                                               string metadata_compression_codec, bool skip_schema_inference,
                                               int64_t data_cardinality, int64_t delete_cardinality) {
    
	auto cardinality = make_uniq<ComparisonExpression>(ExpressionType::COMPARE_EQUAL, make_uniq<ColumnRefExpression>("explicit_cardinality"),
	                                                                                  make_uniq<ConstantExpression>(Value(data_cardinality)));

	// No deletes, just return a TableFunctionRef for a parquet scan of the data files
	if (delete_file_values.empty()) {
		auto table_function_ref_data = make_uniq<TableFunctionRef>();
		table_function_ref_data->alias = "iceberg_scan_data";
		vector<unique_ptr<ParsedExpression>> left_children;
		left_children.push_back(make_uniq<ConstantExpression>(Value::LIST(data_file_values)));
		left_children.push_back(std::move(cardinality));
		if (!skip_schema_inference) {
			left_children.push_back(
					make_uniq<ComparisonExpression>(ExpressionType::COMPARE_EQUAL, make_uniq<ColumnRefExpression>("schema"),
					make_uniq<ConstantExpression>(GetParquetSchemaParam(schema))));
		}

		table_function_ref_data->function = make_uniq<FunctionExpression>("parquet_scan", std::move(left_children));
		return std::move(table_function_ref_data);
	}

	// Join
	auto join_node = make_uniq<JoinRef>(JoinRefType::REGULAR);
	auto filename_match_expr =
	    allow_moved_paths
	        ? GetFilenameMatchExpr()
	        : make_uniq<ComparisonExpression>(ExpressionType::COMPARE_NOT_DISTINCT_FROM,
	                                          make_uniq<ColumnRefExpression>("filename", "iceberg_scan_data"),
	                                          make_uniq<ColumnRefExpression>("file_path", "iceberg_scan_deletes"));
	join_node->type = JoinType::ANTI;
	join_node->condition = make_uniq<ConjunctionExpression>(
	    ExpressionType::CONJUNCTION_AND, std::move(filename_match_expr),
	    make_uniq<ComparisonExpression>(ExpressionType::COMPARE_NOT_DISTINCT_FROM,
	                                    make_uniq<ColumnRefExpression>("file_row_number", "iceberg_scan_data"),
	                                    make_uniq<ColumnRefExpression>("pos", "iceberg_scan_deletes")));

	// LHS: data
	auto table_function_ref_data = make_uniq<TableFunctionRef>();
	table_function_ref_data->alias = "iceberg_scan_data";
	vector<unique_ptr<ParsedExpression>> left_children;
	left_children.push_back(make_uniq<ConstantExpression>(Value::LIST(data_file_values)));
	left_children.push_back(std::move(cardinality));
	left_children.push_back(make_uniq<ComparisonExpression>(ExpressionType::COMPARE_EQUAL,
	                                                        make_uniq<ColumnRefExpression>("filename"),
	                                                        make_uniq<ConstantExpression>(Value(1))));
	left_children.push_back(make_uniq<ComparisonExpression>(ExpressionType::COMPARE_EQUAL,
	                                                        make_uniq<ColumnRefExpression>("file_row_number"),
	                                                        make_uniq<ConstantExpression>(Value(1))));
	if (!skip_schema_inference) {
		left_children.push_back(
			make_uniq<ComparisonExpression>(ExpressionType::COMPARE_EQUAL, make_uniq<ColumnRefExpression>("schema"),
			make_uniq<ConstantExpression>(GetParquetSchemaParam(schema))));
	}
	table_function_ref_data->function = make_uniq<FunctionExpression>("parquet_scan", std::move(left_children));
	join_node->left = std::move(table_function_ref_data);

	// RHS: deletes
	auto table_function_ref_deletes = make_uniq<TableFunctionRef>();
	table_function_ref_deletes->alias = "iceberg_scan_deletes";
	vector<unique_ptr<ParsedExpression>> right_children;
	right_children.push_back(make_uniq<ConstantExpression>(Value::LIST(delete_file_values)));
	right_children.push_back(make_uniq<ComparisonExpression>(ExpressionType::COMPARE_EQUAL, make_uniq<ColumnRefExpression>("explicit_cardinality"),
	                                                                                        make_uniq<ConstantExpression>(Value(delete_cardinality))));
	table_function_ref_deletes->function = make_uniq<FunctionExpression>("parquet_scan", std::move(right_children));
	join_node->right = std::move(table_function_ref_deletes);

	// Wrap the join in a select, exclude the filename and file_row_number cols
	auto select_statement = make_uniq<SelectStatement>();

	// Construct Select node
	auto select_node = make_uniq<SelectNode>();
	select_node->from_table = std::move(join_node);
	auto select_expr = make_uniq<StarExpression>();
	select_expr->exclude_list = {"filename", "file_row_number"};
	vector<unique_ptr<ParsedExpression>> select_exprs;
	select_exprs.push_back(std::move(select_expr));
	select_node->select_list = std::move(select_exprs);
	select_statement->node = std::move(select_node);

	return make_uniq<SubqueryRef>(std::move(select_statement), "iceberg_scan");
}

static unique_ptr<TableRef> IcebergScanBindReplace(ClientContext &context, TableFunctionBindInput &input) {
	FileSystem &fs = FileSystem::GetFileSystem(context);
	auto iceberg_path = input.inputs[0].ToString();

	// Enabling this will ensure the ANTI Join with the deletes only looks at filenames, instead of full paths
	// this allows hive tables to be moved and have mismatching paths, usefull for testing, but will have worse
	// performance
	bool allow_moved_paths = false;
	bool skip_schema_inference = false;
	string mode = "default";
	string metadata_compression_codec = "none";
	string table_version = DEFAULT_TABLE_VERSION;
	string version_name_format = DEFAULT_TABLE_VERSION_FORMAT;

	for (auto &kv : input.named_parameters) {
		auto loption = StringUtil::Lower(kv.first);
		if (loption == "allow_moved_paths") {
			allow_moved_paths = BooleanValue::Get(kv.second);
			if (StringUtil::EndsWith(iceberg_path, ".json")) {
				throw InvalidInputException(
				    "Enabling allow_moved_paths is not enabled for directly scanning metadata files.");
			}
		} else if (loption == "mode") {
			mode = StringValue::Get(kv.second);
		} else if (loption == "metadata_compression_codec") {
			metadata_compression_codec = StringValue::Get(kv.second);
		} else if (loption == "skip_schema_inference") {
			skip_schema_inference = BooleanValue::Get(kv.second);
		} else if (loption == "version") {
			table_version = StringValue::Get(kv.second);
		} else if (loption == "version_name_format") {
			version_name_format = StringValue::Get(kv.second);
		}
	}
	auto iceberg_meta_path = IcebergSnapshot::GetMetaDataPath(iceberg_path, fs, metadata_compression_codec, table_version, version_name_format);
	IcebergSnapshot snapshot_to_scan;
	if (input.inputs.size() > 1) {
		if (input.inputs[1].type() == LogicalType::UBIGINT) {
			snapshot_to_scan = IcebergSnapshot::GetSnapshotById(iceberg_meta_path, fs, input.inputs[1].GetValue<uint64_t>(), metadata_compression_codec, skip_schema_inference);
		} else if (input.inputs[1].type() == LogicalType::TIMESTAMP) {
			snapshot_to_scan =
			    IcebergSnapshot::GetSnapshotByTimestamp(iceberg_meta_path, fs, input.inputs[1].GetValue<timestamp_t>(), metadata_compression_codec, skip_schema_inference);
		} else {
			throw InvalidInputException("Unknown argument type in IcebergScanBindReplace.");
		}
	} else {
		snapshot_to_scan = IcebergSnapshot::GetLatestSnapshot(iceberg_meta_path, fs, metadata_compression_codec, skip_schema_inference);
	}

	IcebergTable iceberg_table = IcebergTable::Load(iceberg_path, snapshot_to_scan, fs, allow_moved_paths, metadata_compression_codec);
	auto data_files = iceberg_table.GetPaths<IcebergManifestContentType::DATA>();
	auto delete_files = iceberg_table.GetPaths<IcebergManifestContentType::DELETE>();
	vector<Value> data_file_values;
	for (auto &data_file : data_files) {
		data_file_values.push_back(
		    {allow_moved_paths ? IcebergUtils::GetFullPath(iceberg_path, data_file, fs) : data_file});
	}
	vector<Value> delete_file_values;
	for (auto &delete_file : delete_files) {
		delete_file_values.push_back(
		    {allow_moved_paths ? IcebergUtils::GetFullPath(iceberg_path, delete_file, fs) : delete_file});
	}

	if (mode == "list_files") {
		return MakeListFilesExpression(data_file_values, delete_file_values);
	} else if (mode == "default") {
		int64_t data_cardinality = 0, delete_cardinality = 0;
		for(auto &manifest : iceberg_table.entries) {
			for(auto &entry : manifest.manifest_entries) {
				if (entry.status != IcebergManifestEntryStatusType::DELETED) {
					if (entry.content == IcebergManifestEntryContentType::DATA) {
						data_cardinality += entry.record_count;
					} else { // DELETES
						delete_cardinality += entry.record_count;
					}
				}
			}
		}
		return MakeScanExpression(data_file_values, delete_file_values, snapshot_to_scan.schema, allow_moved_paths, metadata_compression_codec, skip_schema_inference, data_cardinality, delete_cardinality);
	} else {
		throw NotImplementedException("Unknown mode type for ICEBERG_SCAN bind : '" + mode + "'");
	}
}

TableFunctionSet IcebergFunctions::GetIcebergScanFunction() {
	TableFunctionSet function_set("iceberg_scan");

	auto fun = TableFunction({LogicalType::VARCHAR}, nullptr, nullptr, IcebergScanGlobalTableFunctionState::Init);
	fun.bind_replace = IcebergScanBindReplace;
	fun.named_parameters["skip_schema_inference"] = LogicalType::BOOLEAN;
	fun.named_parameters["allow_moved_paths"] = LogicalType::BOOLEAN;
	fun.named_parameters["mode"] = LogicalType::VARCHAR;
	fun.named_parameters["metadata_compression_codec"] = LogicalType::VARCHAR;
	fun.named_parameters["version"] = LogicalType::VARCHAR;
	fun.named_parameters["version_name_format"] = LogicalType::VARCHAR;
	function_set.AddFunction(fun);

	fun = TableFunction({LogicalType::VARCHAR, LogicalType::UBIGINT}, nullptr, nullptr,
	                    IcebergScanGlobalTableFunctionState::Init);
	fun.bind_replace = IcebergScanBindReplace;
	fun.named_parameters["skip_schema_inference"] = LogicalType::BOOLEAN;
	fun.named_parameters["allow_moved_paths"] = LogicalType::BOOLEAN;
	fun.named_parameters["mode"] = LogicalType::VARCHAR;
	fun.named_parameters["metadata_compression_codec"] = LogicalType::VARCHAR;
	fun.named_parameters["version"] = LogicalType::VARCHAR;
	fun.named_parameters["version_name_format"] = LogicalType::VARCHAR;
	function_set.AddFunction(fun);

	fun = TableFunction({LogicalType::VARCHAR, LogicalType::TIMESTAMP}, nullptr, nullptr,
	                    IcebergScanGlobalTableFunctionState::Init);
	fun.bind_replace = IcebergScanBindReplace;
	fun.named_parameters["skip_schema_inference"] = LogicalType::BOOLEAN;
	fun.named_parameters["allow_moved_paths"] = LogicalType::BOOLEAN;
	fun.named_parameters["mode"] = LogicalType::VARCHAR;
	fun.named_parameters["metadata_compression_codec"] = LogicalType::VARCHAR;
	fun.named_parameters["version"] = LogicalType::VARCHAR;
	fun.named_parameters["version_name_format"] = LogicalType::VARCHAR;
	function_set.AddFunction(fun);

	return function_set;
}

} // namespace duckdb
