#include "iceberg_functions.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/parser/expression/function_expression.hpp"

namespace duckdb {

vector<TableFunctionSet> IcebergFunctions::GetTableFunctions() {
	vector<TableFunctionSet> functions;

	functions.push_back(GetIcebergSnapshotsFunction());
	functions.push_back(GetIcebergScanFunction());
	functions.push_back(GetIcebergMetadataFunction());

	return functions;
}

vector<ScalarFunction> IcebergFunctions::GetScalarFunctions() {
	vector<ScalarFunction> functions;

	return functions;
}

} // namespace duckdb
