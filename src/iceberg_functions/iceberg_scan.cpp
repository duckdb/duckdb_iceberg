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
#include "duckdb/common/printer.hpp"
#include "iceberg_metadata.hpp"
#include "iceberg_utils.hpp"
#include "iceberg_functions.hpp"
#include "yyjson.hpp"

#include <string>
#include <numeric>
#include <iostream>

namespace duckdb {

// === Derived TableFunctionInfo to hold constraints ===
struct IcebergTableFunctionInfo : public TableFunctionInfo {
    vector<unique_ptr<ParsedExpression>> constraints;

    IcebergTableFunctionInfo(vector<unique_ptr<ParsedExpression>> &&constraints_p)
        : constraints(std::move(constraints_p)) {}
};

// === Helper function to recursively extract comparison predicates from expressions ===
static void ExtractPredicates(ParsedExpression &expr, vector<unique_ptr<ParsedExpression>> &predicates) {
    if (expr.type == ExpressionType::CONJUNCTION_AND) {
        auto &conj = (ConjunctionExpression &)expr;
        // Access children instead of left and right
        if (conj.children.size() >= 2) {
            ExtractPredicates(*conj.children[0], predicates);
            ExtractPredicates(*conj.children[1], predicates);
        }
    } else if (expr.type == ExpressionType::COMPARE_EQUAL ||
               expr.type == ExpressionType::COMPARE_GREATERTHAN ||
               expr.type == ExpressionType::COMPARE_GREATERTHANOREQUALTO ||
               expr.type == ExpressionType::COMPARE_LESSTHAN ||
               expr.type == ExpressionType::COMPARE_LESSTHANOREQUALTO) {
        // Clone the expression and add to predicates
        predicates.emplace_back(expr.Copy());
    }
    // Add more conditions here if you want to handle OR or other expressions
}

struct IcebergScanGlobalTableFunctionState : public GlobalTableFunctionState {
public:
    static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
        return make_uniq<GlobalTableFunctionState>();
    }
};

static unique_ptr<ParsedExpression> GetFilenameExpr(unique_ptr<ColumnRefExpression> colref_expr) {
    vector<unique_ptr<ParsedExpression>> split_children;
    split_children.emplace_back(std::move(colref_expr));
    split_children.emplace_back(make_uniq<ConstantExpression>(Value("/")));
    auto data_split = make_uniq<FunctionExpression>("string_split", std::move(split_children));

    vector<unique_ptr<ParsedExpression>> list_extract_children;
    list_extract_children.emplace_back(std::move(data_split));
    list_extract_children.emplace_back(make_uniq<ConstantExpression>(Value(-1)));
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

// Uses recursive unnest on list of structs to return a table with all data and delete files
static unique_ptr<TableRef> MakeListFilesExpression(vector<Value> &data_file_values,
                                                    vector<Value> &delete_file_values) {
    vector<Value> structs;
    for (const auto &file : data_file_values) {
        child_list_t<Value> child;
        child.emplace_back(make_pair("file", file));
        child.emplace_back(make_pair("type", Value("data")));
        structs.emplace_back(Value::STRUCT(child));
    }
    for (const auto &file : delete_file_values) {
        child_list_t<Value> child;
        child.emplace_back(make_pair("file", file));
        child.emplace_back(make_pair("type", Value("delete")));
        structs.emplace_back(Value::STRUCT(child));
    }

    // Unnest
    vector<unique_ptr<ParsedExpression>> unnest_children;
    unnest_children.emplace_back(make_uniq<ConstantExpression>(Value::LIST(structs)));
    auto recursive_named_param = make_uniq<ConstantExpression>(Value::BOOLEAN(true));
    recursive_named_param->alias = "recursive";
    unnest_children.emplace_back(std::move(recursive_named_param));

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
        map_value_children.emplace_back(make_pair("name", Value(schema_entry.name)));
        map_value_children.emplace_back(make_pair("type", Value(schema_entry.type.ToString())));
        map_value_children.emplace_back(make_pair("default_value", schema_entry.default_value));
        auto map_value = Value::STRUCT(map_value_children);

        child_list_t<Value> map_entry_children;
        map_entry_children.emplace_back(make_pair("key", Value(schema_entry.id)));
        map_entry_children.emplace_back(make_pair("value", map_value));
        auto map_entry = Value::STRUCT(map_entry_children);

        map_entries.emplace_back(map_entry);
    }

    auto param_type =
        LogicalType::STRUCT({{"key", LogicalType::INTEGER},
                             {"value", LogicalType::STRUCT({{{"name", LogicalType::VARCHAR},
                                                             {"type", LogicalType::VARCHAR},
                                                             {"default_value", LogicalType::VARCHAR}}})}});
    auto ret = Value::MAP(param_type, map_entries);
    return ret;
}

// Enhanced EvaluatePredicateAgainstStatistics
static bool EvaluatePredicateAgainstStatistics(const IcebergManifestEntry &entry, const vector<unique_ptr<ParsedExpression>> &predicates) {
    for (const auto &predicate : predicates) {
        if (auto comparison = dynamic_cast<ComparisonExpression *>(predicate.get())) {
            // Assume predicates are on columns, possibly transformed
            // Map 'date' to 'date_day' if necessary
            string column_name;
            unique_ptr<ParsedExpression> transformed_predicate;

            if (auto colref = dynamic_cast<ColumnRefExpression *>(comparison->left.get())) {
                column_name = colref->GetColumnName();

                // If the predicate is on 'date', map it to 'date_day'
                if (column_name == "date") {
                    column_name = "date_day";
                    // Transform the predicate value accordingly
                    // For simplicity, assume 'date_day' is equivalent to 'date' truncated to day
                    // You might need to adjust this based on your actual transformation
                }
            } else {
                // Unsupported predicate structure
                continue;
            }

            Value constant_value;
            if (auto const_expr = dynamic_cast<ConstantExpression *>(comparison->right.get())) {
                constant_value = const_expr->value;
            } else {
                // Unsupported predicate structure
                continue;
            }

            std::cout << "  Evaluating predicate: " << predicate->ToString() << std::endl;
            std::cout << "    Mapped Column: " << column_name << ", Value: " << constant_value.ToString() << std::endl;

            // Check if we have lower and upper bounds for this column
            if (entry.lower_bounds.find(column_name) != entry.lower_bounds.end() &&
                entry.upper_bounds.find(column_name) != entry.upper_bounds.end()) {
                const auto &lower_bound = entry.lower_bounds.at(column_name);
                const auto &upper_bound = entry.upper_bounds.at(column_name);

                std::cout << "    Column: " << column_name 
                          << ", Lower bound: " << lower_bound.ToString()
                          << ", Upper bound: " << upper_bound.ToString()
                          << std::endl;

                bool result = true;
                switch (comparison->type) {
                    case ExpressionType::COMPARE_EQUAL:
                        result = (constant_value >= lower_bound && constant_value <= upper_bound);
                        break;
                    case ExpressionType::COMPARE_GREATERTHAN:
                        result = (constant_value < upper_bound);
                        break;
                    case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
                        result = (constant_value <= upper_bound);
                        break;
                    case ExpressionType::COMPARE_LESSTHAN:
                        result = (constant_value > lower_bound);
                        break;
                    case ExpressionType::COMPARE_LESSTHANOREQUALTO:
                        result = (constant_value >= lower_bound);
                        break;
                    default:
                        // For other types of comparisons, we can't make a decision based on bounds
                        result = true; // Conservative approach
                        break;
                }
                std::cout << "    Predicate evaluation result: " << (result ? "true" : "false") << std::endl;
                if (!result) {
                    return false;
                }
            } else {
                std::cout << "    No bounds found for column: " << column_name << std::endl;
                // Without bounds, we cannot exclude the file
            }
        }
    }
    return true;
}

// Build the Parquet Scan expression for the files we need to scan
static unique_ptr<TableRef> MakeScanExpression(const string &iceberg_path, FileSystem &fs,
                                               vector<IcebergManifestEntry> &data_file_entries,
                                               vector<Value> &delete_file_values,
                                               vector<IcebergColumnDefinition> &schema, bool allow_moved_paths,
                                               string metadata_compression_codec, bool skip_schema_inference,
                                               const IcebergTableFunctionInfo *iceberg_info = nullptr) {
    // Log the total number of files before filtering
    std::cout << "Iceberg scan: Total data files before filtering: " << data_file_entries.size() << std::endl;
    if (iceberg_info) {
        std::cout << "Iceberg scan: iceberg_info provided." << std::endl;
        std::cout << "Iceberg scan: Number of constraints: " << iceberg_info->constraints.size() << std::endl;
    } else {
        std::cout << "Iceberg scan: iceberg_info is nullptr." << std::endl;
    }

    // Log predicates if they exist
    if (iceberg_info && !iceberg_info->constraints.empty()) {
        std::cout << "Iceberg scan: Predicates applied:" << std::endl;
        for (const auto &predicate : iceberg_info->constraints) {
            std::cout << "  " << predicate->ToString() << std::endl;
        }
    } else {
        std::cout << "Iceberg scan: No predicates applied" << std::endl;
    }

    vector<Value> filtered_data_file_values;
    if (iceberg_info && !iceberg_info->constraints.empty()) {
        for (const auto &entry : data_file_entries) {
            std::cout << "Evaluating file: " << entry.file_path << std::endl;
            if (EvaluatePredicateAgainstStatistics(entry, iceberg_info->constraints)) {
                auto full_path = allow_moved_paths ? IcebergUtils::GetFullPath(iceberg_path, entry.file_path, fs) : entry.file_path;
                filtered_data_file_values.emplace_back(full_path);
                std::cout << "  Iceberg scan: Data file included after filtering: " << full_path << std::endl;
            } else {
                std::cout << "  Iceberg scan: Data file excluded after filtering: " << entry.file_path << std::endl;
            }
        }
        std::cout << "Iceberg scan: Data files after filtering: " << filtered_data_file_values.size() << std::endl;
    } else {
        for (const auto &entry : data_file_entries) {
            auto full_path = allow_moved_paths ? IcebergUtils::GetFullPath(iceberg_path, entry.file_path, fs) : entry.file_path;
            filtered_data_file_values.emplace_back(full_path);
        }
        std::cout << "Iceberg scan: No predicates applied, all " << filtered_data_file_values.size() << " files included" << std::endl;
    }

    // Log delete files
    std::cout << "Iceberg scan: Delete files: " << delete_file_values.size() << std::endl;

    // No deletes, just return a TableFunctionRef for a parquet scan of the data files
    if (delete_file_values.empty()) {
        auto table_function_ref_data = make_uniq<TableFunctionRef>();
        table_function_ref_data->alias = "iceberg_scan_data";
        vector<unique_ptr<ParsedExpression>> left_children;
        left_children.emplace_back(make_uniq<ConstantExpression>(Value::LIST(filtered_data_file_values)));
        if (!skip_schema_inference) {
            left_children.emplace_back(
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
    left_children.emplace_back(make_uniq<ConstantExpression>(Value::LIST(filtered_data_file_values)));
    left_children.emplace_back(make_uniq<ComparisonExpression>(ExpressionType::COMPARE_EQUAL,
                                                           make_uniq<ColumnRefExpression>("filename"),
                                                           make_uniq<ConstantExpression>(Value(1))));
    left_children.emplace_back(make_uniq<ComparisonExpression>(ExpressionType::COMPARE_EQUAL,
                                                           make_uniq<ColumnRefExpression>("file_row_number"),
                                                           make_uniq<ConstantExpression>(Value(1))));
    if (!skip_schema_inference) {
        left_children.emplace_back(
            make_uniq<ComparisonExpression>(ExpressionType::COMPARE_EQUAL, make_uniq<ColumnRefExpression>("schema"),
            make_uniq<ConstantExpression>(GetParquetSchemaParam(schema))));
    }
    table_function_ref_data->function = make_uniq<FunctionExpression>("parquet_scan", std::move(left_children));
    join_node->left = std::move(table_function_ref_data);

    // RHS: deletes
    auto table_function_ref_deletes = make_uniq<TableFunctionRef>();
    table_function_ref_deletes->alias = "iceberg_scan_deletes";
    vector<unique_ptr<ParsedExpression>> right_children;
    right_children.emplace_back(make_uniq<ConstantExpression>(Value::LIST(delete_file_values)));
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
    select_exprs.emplace_back(std::move(select_expr));
    select_node->select_list = std::move(select_exprs);
    select_statement->node = std::move(select_node);

    return make_uniq<SubqueryRef>(std::move(select_statement), "iceberg_scan");
}

static unique_ptr<TableRef> IcebergScanBindReplace(ClientContext &context, TableFunctionBindInput &input) {
    FileSystem &fs = FileSystem::GetFileSystem(context);
    auto iceberg_path = input.inputs[0].ToString();

    // Log the input path
    std::cout << "Iceberg scan: Input path: " << iceberg_path << std::endl;

    // Enabling this will ensure the ANTI Join with the deletes only looks at filenames, instead of full paths
    // this allows hive tables to be moved and have mismatching paths, useful for testing, but will have worse
    // performance
    bool allow_moved_paths = false;
    bool skip_schema_inference = false;
    string mode = "default";
    string metadata_compression_codec = "none";
    string table_version = DEFAULT_VERSION_HINT_FILE;
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
    
    // Log the metadata path
    std::cout << "Iceberg scan: Metadata path: " << iceberg_meta_path << std::endl;

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

    // Log some information about the loaded table
    std::cout << "Iceberg scan: Loaded table with " << iceberg_table.entries.size() << " entries" << std::endl;

    auto data_entries = iceberg_table.GetEntries<IcebergManifestContentType::DATA>();
    auto delete_files = iceberg_table.GetPaths<IcebergManifestContentType::DELETE>();

    // Log information about data entries and delete files
    std::cout << "Iceberg scan: Found " << data_entries.size() << " data entries" << std::endl;
    std::cout << "Iceberg scan: Found " << delete_files.size() << " delete files" << std::endl;

    vector<Value> delete_file_values;
    for (auto &delete_file : delete_files) {
        auto full_path = allow_moved_paths ? IcebergUtils::GetFullPath(iceberg_path, delete_file, fs) : delete_file;
        delete_file_values.emplace_back(full_path);
        
        // Log each delete file path
        std::cout << "Iceberg scan: Delete file: " << full_path << std::endl;
    }

    // === Extract predicates from input.info (if possible) ===
    vector<unique_ptr<ParsedExpression>> extracted_predicates;
    if (input.info) {
        // Attempt to cast to IcebergTableFunctionInfo
        auto iceberg_info_cast = dynamic_cast<IcebergTableFunctionInfo *>(input.info.get());
        if (iceberg_info_cast && !iceberg_info_cast->constraints.empty()) {
            for (auto &constraint : iceberg_info_cast->constraints) {
                ExtractPredicates(*constraint, extracted_predicates);
            }
        }
    }

    // Create IcebergTableFunctionInfo with extracted predicates
    auto iceberg_info = make_uniq<IcebergTableFunctionInfo>(std::move(extracted_predicates));
    input.info = iceberg_info.release(); // Assign raw pointer

    if (mode == "list_files") {
        vector<Value> data_file_values;
        for (const auto &entry : data_entries) {
            auto full_path = allow_moved_paths ? IcebergUtils::GetFullPath(iceberg_path, entry.file_path, fs) : entry.file_path;
            data_file_values.emplace_back(full_path);
        }
        return MakeListFilesExpression(data_file_values, delete_file_values);
    } else if (mode == "default") {
        // Pass the extracted predicates to MakeScanExpression
        // Cast input.info to IcebergTableFunctionInfo to access constraints
        IcebergTableFunctionInfo *iceberg_info_cast = dynamic_cast<IcebergTableFunctionInfo *>(input.info.get());
        return MakeScanExpression(iceberg_path, fs, data_entries, delete_file_values, snapshot_to_scan.schema, allow_moved_paths, 
                                  metadata_compression_codec, skip_schema_inference, iceberg_info_cast);
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
