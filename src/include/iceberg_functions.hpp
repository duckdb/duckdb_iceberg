//===----------------------------------------------------------------------===//
//                         DuckDB
//
// iceberg_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_copy_function_info.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

namespace duckdb {

class IcebergFunctions {
public:
	static vector<CreateTableFunctionInfo> GetTableFunctions();
	static vector<CreateScalarFunctionInfo> GetScalarFunctions();

private:
	static CreateTableFunctionInfo GetIcebergSnapshotsFunction();
	static CreateTableFunctionInfo GetIcebergScanFunction();
};

} // namespace duckdb
