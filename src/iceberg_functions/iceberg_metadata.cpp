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
#include "iceberg_functions.hpp"
#include "yyjson.hpp"

#include <string>
#include <numeric>

namespace duckdb {

struct IcebergMetaDataBindData : public TableFunctionData {
	unique_ptr<IcebergTable> iceberg_table;
};

struct IcebergMetaDataGlobalTableFunctionState : public GlobalTableFunctionState {
public:
	IcebergMetaDataGlobalTableFunctionState() {

	};

	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
		return make_uniq<IcebergMetaDataGlobalTableFunctionState>();
	}

	idx_t current_manifest_idx = 0;
	idx_t current_manifest_entry_idx = 0;
};

static unique_ptr<FunctionData> IcebergMetaDataBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
	// return a TableRef that contains the scans for the
	auto ret = make_uniq<IcebergMetaDataBindData>();

	FileSystem &fs = FileSystem::GetFileSystem(context);
	auto iceberg_path = input.inputs[0].ToString();

	bool allow_moved_paths = false;

	for (auto &kv : input.named_parameters) {
		auto loption = StringUtil::Lower(kv.first);
		if (loption == "allow_moved_paths") {
			allow_moved_paths = BooleanValue::Get(kv.second);
		}
	}

	IcebergSnapshot snapshot_to_scan;
	if (input.inputs.size() > 1) {
		if (input.inputs[1].type() == LogicalType::UBIGINT) {
			snapshot_to_scan = IcebergSnapshot::GetSnapshotById(iceberg_path, fs, input.inputs[1].GetValue<uint64_t>());
		} else if (input.inputs[1].type() == LogicalType::TIMESTAMP) {
			snapshot_to_scan =
			    IcebergSnapshot::GetSnapshotByTimestamp(iceberg_path, fs, input.inputs[1].GetValue<timestamp_t>());
		} else {
			throw InvalidInputException("Unknown argument type in IcebergScanBindReplace.");
		}
	} else {
		snapshot_to_scan = IcebergSnapshot::GetLatestSnapshot(iceberg_path, fs);
	}

	ret->iceberg_table =
	    make_uniq<IcebergTable>(IcebergTable::Load(iceberg_path, snapshot_to_scan, fs, allow_moved_paths));

	auto manifest_types = IcebergManifest::Types();
	return_types.insert(return_types.end(), manifest_types.begin(), manifest_types.end());
	auto manifest_entry_types = IcebergManifestEntry::Types();
	return_types.insert(return_types.end(), manifest_entry_types.begin(), manifest_entry_types.end());

	auto manifest_names = IcebergManifest::Names();
	names.insert(names.end(), manifest_names.begin(), manifest_names.end());
	auto manifest_entry_names = IcebergManifestEntry::Names();
	names.insert(names.end(), manifest_entry_names.begin(), manifest_entry_names.end());

	return std::move(ret);
}

static void IcebergMetaDataFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<IcebergMetaDataBindData>();
	auto &global_state = data.global_state->Cast<IcebergMetaDataGlobalTableFunctionState>();

	idx_t out = 0;
	auto manifests = bind_data.iceberg_table->entries;
	for (; global_state.current_manifest_idx < manifests.size(); global_state.current_manifest_idx++) {
		auto manifest_entries = manifests[global_state.current_manifest_idx].manifest_entries;
		for (; global_state.current_manifest_entry_idx < manifest_entries.size();
		     global_state.current_manifest_entry_idx++) {
			if (out >= STANDARD_VECTOR_SIZE) {
				output.SetCardinality(out);
				return;
			}
			auto manifest = manifests[global_state.current_manifest_idx];
			auto manifest_entry = manifest_entries[global_state.current_manifest_entry_idx];

			FlatVector::GetData<string_t>(output.data[0])[out] =
			    StringVector::AddString(output.data[0], string_t(manifest.manifest.manifest_path));
			;
			FlatVector::GetData<int64_t>(output.data[1])[out] = manifest.manifest.sequence_number;
			FlatVector::GetData<string_t>(output.data[2])[out] = StringVector::AddString(
			    output.data[2], string_t(IcebergManifestContentTypeToString(manifest.manifest.content)));

			FlatVector::GetData<string_t>(output.data[3])[out] = StringVector::AddString(
			    output.data[3], string_t(IcebergManifestEntryStatusTypeToString(manifest_entry.status)));
			FlatVector::GetData<string_t>(output.data[4])[out] = StringVector::AddString(
			    output.data[4], string_t(IcebergManifestEntryContentTypeToString(manifest_entry.content)));
			FlatVector::GetData<string_t>(output.data[5])[out] =
			    StringVector::AddString(output.data[5], string_t(manifest_entry.file_path));
			FlatVector::GetData<string_t>(output.data[6])[out] =
			    StringVector::AddString(output.data[6], string_t(manifest_entry.file_format));
			FlatVector::GetData<int64_t>(output.data[7])[out] = manifest_entry.record_count;
			out++;
		}
		global_state.current_manifest_entry_idx = 0;
	}
	output.SetCardinality(out);
}

TableFunctionSet IcebergFunctions::GetIcebergMetadataFunction() {
	TableFunctionSet function_set("iceberg_metadata");

	auto fun = TableFunction({LogicalType::VARCHAR}, IcebergMetaDataFunction, IcebergMetaDataBind,
	                         IcebergMetaDataGlobalTableFunctionState::Init);
	fun.named_parameters["allow_moved_paths"] = LogicalType::BOOLEAN;
	function_set.AddFunction(fun);

	fun = TableFunction({LogicalType::VARCHAR, LogicalType::UBIGINT}, IcebergMetaDataFunction, IcebergMetaDataBind,
	                    IcebergMetaDataGlobalTableFunctionState::Init);
	fun.named_parameters["allow_moved_paths"] = LogicalType::BOOLEAN;
	function_set.AddFunction(fun);

	fun = TableFunction({LogicalType::VARCHAR, LogicalType::TIMESTAMP}, IcebergMetaDataFunction, IcebergMetaDataBind,
	                    IcebergMetaDataGlobalTableFunctionState::Init);
	fun.named_parameters["allow_moved_paths"] = LogicalType::BOOLEAN;
	function_set.AddFunction(fun);

	return function_set;
}

} // namespace duckdb
