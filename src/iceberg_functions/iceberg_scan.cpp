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
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
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
#include <iomanip> // For std::setw

namespace duckdb {

// Helper function to log indentation
static void LogIndent(int indent_level) {
    for (int i = 0; i < indent_level; ++i) {
        std::cout << "  ";
    }
}

// Utility function to convert ExpressionType enum to string using DuckDB's function
static std::string GetExpressionTypeString(ExpressionType type) {
    return ExpressionTypeToString(type); // Use DuckDB's existing function
}

// Recursive function to log details of a ParsedExpression
static void LogExpressionDetails(const ParsedExpression &expr, int indent_level = 0) {
    LogIndent(indent_level);
    std::cout << "Expression Type: " << GetExpressionTypeString(expr.type) << std::endl;
    
    switch (expr.type) {
        case ExpressionType::CONJUNCTION_AND:
        case ExpressionType::CONJUNCTION_OR: {
            auto &conj = (ConjunctionExpression &)expr;
            std::cout << "Conjunction Type: " << GetExpressionTypeString(conj.type) << std::endl;
            std::cout << "Number of Children: " << conj.children.size() << std::endl;
            for (const auto &child : conj.children) {
                LogExpressionDetails(*child, indent_level + 1);
            }
            break;
        }
        case ExpressionType::COMPARE_EQUAL:
        case ExpressionType::COMPARE_GREATERTHAN:
        case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
        case ExpressionType::COMPARE_LESSTHAN:
        case ExpressionType::COMPARE_LESSTHANOREQUALTO: {
            auto &comp = (ComparisonExpression &)expr;
            std::cout << "Comparison Operator: " << GetExpressionTypeString(comp.type) << std::endl;
            LogIndent(indent_level);
            std::cout << "Left Operand:" << std::endl;
            LogExpressionDetails(*comp.left, indent_level + 1);
            LogIndent(indent_level);
            std::cout << "Right Operand:" << std::endl;
            LogExpressionDetails(*comp.right, indent_level + 1);
            break;
        }
        case ExpressionType::COLUMN_REF: {
            auto &col_ref = (ColumnRefExpression &)expr;
            std::cout << "Column Name: " << col_ref.column_names[0] << std::endl;
            break;
        }
        case ExpressionType::VALUE_CONSTANT: { // Corrected
            auto &const_expr = (ConstantExpression &)expr;
            std::cout << "Constant Value: " << const_expr.value.ToString() << std::endl;
            break;
        }
        case ExpressionType::FUNCTION: {
            auto &func_expr = (FunctionExpression &)expr;
            std::cout << "Function Name: " << func_expr.function_name << std::endl;
            std::cout << "Number of Arguments: " << func_expr.children.size() << std::endl;
            for (const auto &child : func_expr.children) {
                LogExpressionDetails(*child, indent_level + 1);
            }
            break;
        }
        // Add more cases as needed for other expression types
        default:
            LogIndent(indent_level);
            std::cout << "Unhandled Expression Type: " << GetExpressionTypeString(expr.type) << std::endl;
            break;
    }
}

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

// Utility function to convert byte vector to hex string for logging
static std::string ByteArrayToHexString(const std::vector<uint8_t> &bytes) {
    std::ostringstream oss;
    for (auto byte : bytes) {
        oss << std::hex << std::setw(2) << std::setfill('0') << (int)byte;
    }
    return oss.str();
}

// Updated DeserializeBound function with detailed logging
static Value DeserializeBound(const std::vector<uint8_t> &bound_value, const LogicalType &type) {
    // Log the type ID and raw bound value
    std::cout << "    DeserializeBound called with Type: " 
              << LogicalTypeIdToString(type.id()) 
              << ", Raw Bound Value (Hex): " << ByteArrayToHexString(bound_value) << std::endl;

    Value deserialized_value;
    try {
        switch (type.id()) {
            case LogicalTypeId::INTEGER: {
                if (bound_value.size() < sizeof(int32_t)) {
                    throw std::runtime_error("Invalid bound size for INTEGER type");
                }
                int32_t val;
                std::memcpy(&val, bound_value.data(), sizeof(int32_t));
                deserialized_value = Value::INTEGER(val);
                break;
            }
            case LogicalTypeId::BIGINT: {
                if (bound_value.size() < sizeof(int64_t)) {
                    throw std::runtime_error("Invalid bound size for BIGINT type");
                }
                int64_t val;
                std::memcpy(&val, bound_value.data(), sizeof(int64_t));
                deserialized_value = Value::BIGINT(val);
                break;
            }
            case LogicalTypeId::DATE: {
                if (bound_value.size() < sizeof(int32_t)) { // Dates are typically stored as int32 (days since epoch)
                    throw std::runtime_error("Invalid bound size for DATE type");
                }
                int32_t days_since_epoch;
                std::memcpy(&days_since_epoch, bound_value.data(), sizeof(int32_t));
                // Convert to DuckDB date
                date_t date = Date::EpochDaysToDate(days_since_epoch);
                deserialized_value = Value::DATE(date);
                break;
            }
            case LogicalTypeId::TIMESTAMP: {
                if (bound_value.size() < sizeof(int64_t)) { // Timestamps are typically stored as int64 (microseconds since epoch)
                    throw std::runtime_error("Invalid bound size for TIMESTAMP type");
                }
                int64_t micros_since_epoch;
                std::memcpy(&micros_since_epoch, bound_value.data(), sizeof(int64_t));
                std::cout << "    TIMESTAMP bound value (microseconds since epoch): " << micros_since_epoch << std::endl;
                // Convert to DuckDB timestamp using microseconds
                timestamp_t timestamp = Timestamp::FromEpochMicroSeconds(micros_since_epoch);
                deserialized_value = Value::TIMESTAMP(timestamp);
                std::cout << "    TIMESTAMP bound value (converted): " << deserialized_value.ToString() << std::endl;
                break;
            }
            case LogicalTypeId::TIMESTAMP_TZ: { // Added support for TIMESTAMP WITH TIME ZONE
                if (bound_value.size() < sizeof(int64_t)) { // Assuming stored as int64 (microseconds since epoch)
                    throw std::runtime_error("Invalid bound size for TIMESTAMP_TZ type");
                }
                int64_t micros_since_epoch;
                std::memcpy(&micros_since_epoch, bound_value.data(), sizeof(int64_t));
                std::cout << "    TIMESTAMP_TZ bound value (microseconds since epoch): " << micros_since_epoch << std::endl;
                // Convert to DuckDB timestamp using microseconds
                timestamp_t timestamp = Timestamp::FromEpochMicroSeconds(micros_since_epoch);
                // Create a TIMESTAMPTZ Value
                deserialized_value = Value::TIMESTAMPTZ(timestamp);
                std::cout << "    TIMESTAMP_TZ bound value (converted): " << deserialized_value.ToString() << std::endl;
                break;
            }
            case LogicalTypeId::DOUBLE: {
                if (bound_value.size() < sizeof(double)) {
                    throw std::runtime_error("Invalid bound size for DOUBLE type");
                }
                double val;
                std::memcpy(&val, bound_value.data(), sizeof(double));
                deserialized_value = Value::DOUBLE(val);
                break;
            }
            case LogicalTypeId::VARCHAR: {
                // Assume the bytes represent a UTF-8 string
                std::string str(bound_value.begin(), bound_value.end());
                deserialized_value = Value(str);
                break;
            }
            // Add more types as needed
            default:
                throw std::runtime_error("Unsupported type for DeserializeBound");
        }

        // Log the final deserialized value
        std::cout << "    Deserialized Value: " << deserialized_value.ToString() << std::endl;
    } catch (const std::exception &e) {
        std::cout << "    Error during deserialization: " << e.what() << std::endl;
        // Depending on your error handling strategy, you might want to rethrow or handle it here
        throw;
    }

    return deserialized_value;
}


static bool EvaluatePredicateAgainstStatistics(const IcebergManifestEntry &entry, 
                                               const vector<unique_ptr<ParsedExpression>> &predicates,
                                               const std::vector<IcebergColumnDefinition> &schema) {
    // Create a mapping from column names to field IDs and their LogicalTypes
    std::unordered_map<std::string, std::pair<int, LogicalType>> column_to_field_info;
    for (const auto &col_def : schema) {
        column_to_field_info[col_def.name] = {col_def.id, col_def.type}; // Assuming col_def.type is LogicalType
    }

    for (const auto &predicate : predicates) {
        if (auto comparison = dynamic_cast<ComparisonExpression *>(predicate.get())) {
            // Assume predicates are on columns, possibly transformed
            std::string column_name;
            if (auto colref = dynamic_cast<ColumnRefExpression *>(comparison->left.get())) {
                column_name = colref->GetColumnName();
            } else {
                // Unsupported predicate structure  
                std::cout << "    Unsupported predicate structure. Skipping predicate." << std::endl;
                continue;
            }

            // Retrieve field ID and type
            auto it = column_to_field_info.find(column_name);
            if (it == column_to_field_info.end()) {
                // Column not found in schema, cannot evaluate predicate
                std::cout << "    Column '" << column_name << "' not found in schema. Skipping predicate." << std::endl;
                continue;
            }
            int field_id = it->second.first;
            LogicalType field_type = it->second.second;

            // Convert field_id to string for lookup
            std::string field_id_str = std::to_string(field_id);

            // Get lower and upper bounds
            auto lower_it = entry.lower_bounds.find(field_id_str);
            auto upper_it = entry.upper_bounds.find(field_id_str);

            if (lower_it == entry.lower_bounds.end() || upper_it == entry.upper_bounds.end()) {
                std::cout << "    No bounds found for field ID: " << field_id_str << ". Cannot evaluate predicate." << std::endl;
                continue; // Cannot filter based on missing bounds
            }

            // Deserialize bounds
            Value lower_bound, upper_bound;
            try {
                lower_bound = DeserializeBound(lower_it->second, field_type);
                upper_bound = DeserializeBound(upper_it->second, field_type);
            } catch (const std::exception &e) {
                std::cout << "    Failed to deserialize bounds for field ID " << field_id_str << ": " << e.what() << std::endl;
                continue;
            }

            // Extract the constant value from the predicate
            Value constant_value;
            if (auto const_expr = dynamic_cast<ConstantExpression *>(comparison->right.get())) {
                constant_value = const_expr->value;
            } else {
                // Unsupported predicate structure
                std::cout << "    Unsupported predicate structure on right operand. Skipping predicate." << std::endl;
                continue;
            }

            std::cout << "  Evaluating predicate: " << predicate->ToString() << std::endl;
            std::cout << "    Mapped Field ID: " << field_id_str << ", Value: " << constant_value.ToString() << std::endl;
            std::cout << "  IcebergManifestEntry bounds for field ID '" << field_id_str << "':" << std::endl;
            std::cout << "    Lower bound: " << lower_bound.ToString() << std::endl;
            std::cout << "    Upper bound: " << upper_bound.ToString() << std::endl;

            // Evaluate the predicate against the bounds
            bool result = true;
            switch (comparison->type) {
                case ExpressionType::COMPARE_EQUAL:
                    result = (constant_value >= lower_bound && constant_value <= upper_bound);
                    break;
                case ExpressionType::COMPARE_GREATERTHAN:
                    result = (constant_value <= upper_bound);
                    break;
                case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
                    result = (constant_value <= upper_bound);
                    break;
                case ExpressionType::COMPARE_LESSTHAN:
                    result = (constant_value >= lower_bound);
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
                return false; // If any predicate fails, exclude the file
            }
        }
    }
    return true; // All predicates passed
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
        int predicate_index = 1;
        for (const auto &predicate : iceberg_info->constraints) {
            std::cout << "  Predicate " << predicate_index++ << ":" << std::endl;
            LogExpressionDetails(*predicate, 2); // Indent level 2 for predicates
        }
    } else {
        std::cout << "Iceberg scan: No predicates applied" << std::endl;
    }

    vector<Value> filtered_data_file_values;
    if (iceberg_info && !iceberg_info->constraints.empty()) {
        for (const auto &entry : data_file_entries) {
            std::cout << "Evaluating file: " << entry.file_path << std::endl;
            if (EvaluatePredicateAgainstStatistics(entry, iceberg_info->constraints, schema)) {
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
    // No deletes, just return a TableFunctionRef for a parquet scan of the data files
    if (delete_file_values.empty()) {
        if (!filtered_data_file_values.empty()) {
            // Existing parquet_scan code
            auto table_function_ref_data = make_uniq<TableFunctionRef>();
            table_function_ref_data->alias = "iceberg_scan_data";
            vector<unique_ptr<ParsedExpression>> left_children;
            LogicalType child_type = LogicalType::VARCHAR;
            left_children.emplace_back(
                make_uniq<ConstantExpression>(
                    Value::LIST(child_type, filtered_data_file_values)
                )
            );
            if (!skip_schema_inference) {
                left_children.emplace_back(
                    make_uniq<ComparisonExpression>(
                        ExpressionType::COMPARE_EQUAL, 
                        make_uniq<ColumnRefExpression>("schema"),
                        make_uniq<ConstantExpression>(GetParquetSchemaParam(schema))
                    )
                );
            }
            table_function_ref_data->function = make_uniq<FunctionExpression>("parquet_scan", std::move(left_children));
            return std::move(table_function_ref_data);
        } else {
            // **BEGIN: Handling Empty Filtered Data Files**
            auto select_node = make_uniq<SelectNode>();
            select_node->where_clause = make_uniq<ConstantExpression>(Value::BOOLEAN(false));

            // Add select expressions for each column based on the schema
            for (const auto &col : schema) {
                // Create a NULL constant of the appropriate type
                auto null_expr = make_uniq<ConstantExpression>(Value(col.type));
                // Alias it to the column name
                null_expr->alias = col.name;
                select_node->select_list.emplace_back(std::move(null_expr));
            }

            // **Add the FROM clause as EmptyTableRef**
            select_node->from_table = make_uniq<EmptyTableRef>();

            // Create a SelectStatement
            auto select_statement = make_uniq<SelectStatement>();
            select_statement->node = std::move(select_node);

            // Create a SubqueryRef with the SelectStatement
            auto table_ref_empty = make_uniq<SubqueryRef>(std::move(select_statement), "empty_scan");

            // Log that we are returning an empty table
            std::cout << "Iceberg scan: No files to scan after filtering. Returning empty table." << std::endl;

            return std::move(table_ref_empty);
            // **END: Handling Empty Filtered Data Files**
        }
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

    // Parse named parameters
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

    std::cout << "Iceberg scan: Got Snapshot" << std::endl;

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

    // === Extract predicates from input.binder ===
	vector<unique_ptr<ParsedExpression>> extracted_predicates;
	if (input.binder) {
    std::cout << "Iceberg scan: input.binder: " << input.binder << std::endl;
    
    // Access the where_clause from the binder
    auto statement = input.binder->GetRootStatement();
    if (statement && statement->type == StatementType::SELECT_STATEMENT) {
        auto &select_statement = (SelectStatement &)*statement;
        if (select_statement.node->type == QueryNodeType::SELECT_NODE) {
            auto &select_node = (SelectNode &)*select_statement.node;
            if (select_node.where_clause) {
				std::cout << "Iceberg scan: select_node.where_clause: " << select_node.where_clause->ToString() << std::endl;
                ExtractPredicates(*select_node.where_clause, extracted_predicates);
            }
        }
    }
    
    // Log the number of extracted predicates
    std::cout << "Iceberg scan: Extracted " << extracted_predicates.size() << " predicates" << std::endl;
    
    // Optionally, you can log details of each extracted predicate
    for (size_t i = 0; i < extracted_predicates.size(); ++i) {
        std::cout << "Predicate " << i + 1 << ": " << extracted_predicates[i]->ToString() << std::endl;
        }
    }

    // Log extracted predicates for debugging
    std::cout << "Iceberg scan: Extracted Predicates:" << std::endl;
    int pred_index = 1;
    for (const auto &pred : extracted_predicates) {
        std::cout << "  Predicate " << pred_index++ << ": " << pred->ToString() << std::endl;
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
        if (!iceberg_info_cast) {
            throw std::bad_cast(); // Handle the error appropriately
        }
        return MakeScanExpression(iceberg_path, fs, data_entries, delete_file_values, snapshot_to_scan.schema, allow_moved_paths, 
                                  metadata_compression_codec, skip_schema_inference, iceberg_info_cast);
    } else {
        throw NotImplementedException("Unknown mode type for ICEBERG_SCAN bind : '" + mode + "'");
    }
}

struct IcebergFunctionData : public FunctionData {
    unique_ptr<ParsedExpression> pushdown_predicate;

    IcebergFunctionData() : pushdown_predicate(nullptr) {}

    // Required: Implement the Copy method
    unique_ptr<FunctionData> Copy() const override {
        if (pushdown_predicate) {
            return make_uniq<IcebergFunctionData>(*pushdown_predicate);
        } else {
            return make_uniq<IcebergFunctionData>();
        }
    }

    // Optional: Implement the Equals method if needed
    bool Equals(const FunctionData &other_p) const override {
        auto &other = dynamic_cast<const IcebergFunctionData &>(other_p);
        if (!pushdown_predicate && !other.pushdown_predicate) {
            return true;
        }
        if (pushdown_predicate && other.pushdown_predicate) {
            return pushdown_predicate->Equals(*other.pushdown_predicate);
        }
        return false;
    }

    // Copy constructor for the Copy method
    IcebergFunctionData(const ParsedExpression &expr) {
        pushdown_predicate = expr.Copy();
    }

    IcebergFunctionData(const unique_ptr<ParsedExpression> &expr) {
        if (expr) {
            pushdown_predicate = expr->Copy();
        }
    }
};


// === Implement the pushdown_complex_filter callback ===
static void IcebergPushdownFilter(ClientContext &context, LogicalGet &get, FunctionData *bind_data, vector<unique_ptr<Expression>> &filters) {
    // Combine all filters into a single conjunction (AND)
    unique_ptr<ParsedExpression> combined_filter;
    if (filters.empty()) {
        combined_filter = nullptr;
    } else if (filters.size() == 1) {
		auto iceberg_data = dynamic_cast<IcebergFunctionData *>(bind_data);
        if (!iceberg_data) {
            throw InternalException("Invalid bind_data in pushdown_complex_filter");
        }
		combined_filter = std::move(iceberg_data->pushdown_predicate);
		std::cout << "Iceberg scan: pushdown_complex_filter: 1 filter" << std::endl;
    } else {
        auto conj = make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
        for (auto &filter : filters) {
			std::cout << "Iceberg scan: pushdown_complex_filter: adding filter" << std::endl;
            // conj->children.emplace_back(std::move(filter));
        }
        combined_filter = std::move(conj);
		std::cout << "Iceberg scan: pushdown_complex_filter: added " << filters.size() << " filters" << std::endl;
    }

    // Store the combined filter in FunctionData
    auto iceberg_data = dynamic_cast<IcebergFunctionData *>(bind_data);
    if (!iceberg_data) {
        throw InternalException("Invalid bind_data in pushdown_complex_filter");
    }

    iceberg_data->pushdown_predicate = std::move(combined_filter);

    // Optionally, log the received predicate
    if (iceberg_data->pushdown_predicate) {
        std::cout << "Iceberg scan: Received pushdown predicate: " << iceberg_data->pushdown_predicate->ToString() << std::endl;
    } else {
        std::cout << "Iceberg scan: No pushdown predicate received." << std::endl;
    }

    // By returning nullptr, we indicate that the predicate has been handled
    // If you wish DuckDB to handle additional transformations, modify here
}

// === Register the TableFunction with Filter Pushdown ===
TableFunctionSet IcebergFunctions::GetIcebergScanFunction() {
    TableFunctionSet function_set("iceberg_scan");

    // Default mode: list all files and apply predicate pushdown
    auto fun = TableFunction({LogicalType::VARCHAR}, nullptr, nullptr, IcebergScanGlobalTableFunctionState::Init);
    fun.bind_replace = IcebergScanBindReplace;

    // Enable filter pushdown
    fun.filter_pushdown = true;

    // Implement the pushdown_complex_filter callback
    fun.pushdown_complex_filter = IcebergPushdownFilter;

    // Register named parameters
    fun.named_parameters["skip_schema_inference"] = LogicalType::BOOLEAN;
    fun.named_parameters["allow_moved_paths"] = LogicalType::BOOLEAN;
    fun.named_parameters["mode"] = LogicalType::VARCHAR;
    fun.named_parameters["metadata_compression_codec"] = LogicalType::VARCHAR;
    fun.named_parameters["version"] = LogicalType::VARCHAR;
    fun.named_parameters["version_name_format"] = LogicalType::VARCHAR;

    function_set.AddFunction(fun);

    // Register additional modes as needed (e.g., with UBIGINT, TIMESTAMP)
    // Example for UBIGINT mode
    fun = TableFunction({LogicalType::VARCHAR, LogicalType::UBIGINT}, nullptr, nullptr,
                        IcebergScanGlobalTableFunctionState::Init);
    fun.bind_replace = IcebergScanBindReplace;
    fun.filter_pushdown = true;
    fun.pushdown_complex_filter = IcebergPushdownFilter;
    fun.named_parameters["skip_schema_inference"] = LogicalType::BOOLEAN;
    fun.named_parameters["allow_moved_paths"] = LogicalType::BOOLEAN;
    fun.named_parameters["mode"] = LogicalType::VARCHAR;
    fun.named_parameters["metadata_compression_codec"] = LogicalType::VARCHAR;
    fun.named_parameters["version"] = LogicalType::VARCHAR;
    fun.named_parameters["version_name_format"] = LogicalType::VARCHAR;
    function_set.AddFunction(fun);

    // Example for TIMESTAMP mode
    fun = TableFunction({LogicalType::VARCHAR, LogicalType::TIMESTAMP}, nullptr, nullptr,
                        IcebergScanGlobalTableFunctionState::Init);
    fun.bind_replace = IcebergScanBindReplace;
    fun.filter_pushdown = true;
    fun.pushdown_complex_filter = IcebergPushdownFilter;
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
