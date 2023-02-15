#define DUCKDB_EXTENSION_MAIN

#include "iceberg_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "yyjson.hpp"

#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

namespace duckdb {

// TODO 2:
// Table Function ICEBERG_SNAPSHOTS('path')
// - Read the version hint
// - Read the Json thing
// - List snapshots with all the info we want

// TODO 2:
// - hello world with avro

inline void JsonHelloWorld(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &name_vector = args.data[0];
    UnaryExecutor::Execute<string_t, string_t>(
        name_vector, result, args.size(),
        [&](string_t json_string) {

          // Read JSON and get root
          yyjson_doc *doc = yyjson_read(json_string.GetDataUnsafe(), json_string.GetSize(), 0);
          yyjson_val *root = yyjson_doc_get_root(doc);

          // Get root["hello"]
          yyjson_val *hello_value = yyjson_obj_get(root, "hello");

          string_t res = StringVector::AddString(result, yyjson_get_str(hello_value), yyjson_get_len(hello_value));

          // Free the doc
          yyjson_doc_free(doc);

          return res;
        });
}

static void LoadInternal(DatabaseInstance &instance) {
	Connection con(instance);
    con.BeginTransaction();

    auto &catalog = Catalog::GetSystemCatalog(*con.context);

    CreateScalarFunctionInfo iceberg_fun_info(
            ScalarFunction("hello_world_yyjson", {LogicalType::VARCHAR}, LogicalType::VARCHAR, JsonHelloWorld));
    iceberg_fun_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
    catalog.CreateFunction(*con.context, &iceberg_fun_info);

    con.Commit();
}

void IcebergExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string IcebergExtension::Name() {
	return "iceberg";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void iceberg_init(duckdb::DatabaseInstance &db) {
	LoadInternal(db);
}

DUCKDB_EXTENSION_API const char *iceberg_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
