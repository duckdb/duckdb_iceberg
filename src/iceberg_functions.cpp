#pragma once

#include "iceberg_functions.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/parser/expression/function_expression.hpp"

namespace duckdb {

vector<CreateTableFunctionInfo> IcebergFunctions::GetTableFunctions() {
  vector<CreateTableFunctionInfo> functions;

  functions.push_back(GetIcebergSnapshotsFunction());

  return functions;
}

}
