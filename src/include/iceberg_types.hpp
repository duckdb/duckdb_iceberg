//===----------------------------------------------------------------------===//
//                         DuckDB
//
// iceberg_types.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <iomanip>  // Add this line for std::setfill and std::setw
#include "avro_codegen/iceberg_manifest_entry_partial.hpp"
#include "avro_codegen/iceberg_manifest_entry_partial_v1.hpp"
#include "avro_codegen/iceberg_manifest_file_partial.hpp"
#include "avro_codegen/iceberg_manifest_file_partial_v1.hpp"

namespace duckdb {

enum class IcebergManifestContentType : uint8_t {
	DATA = 0,
	DELETE = 1,
};

static string IcebergManifestContentTypeToString(IcebergManifestContentType type) {
	switch (type) {
	case IcebergManifestContentType::DATA:
		return "DATA";
	case IcebergManifestContentType::DELETE:
		return "DELETE";
	}
}

enum class IcebergManifestEntryStatusType : uint8_t { EXISTING = 0, ADDED = 1, DELETED = 2 };

static string IcebergManifestEntryStatusTypeToString(IcebergManifestEntryStatusType type) {
	switch (type) {
	case IcebergManifestEntryStatusType::EXISTING:
		return "EXISTING";
	case IcebergManifestEntryStatusType::ADDED:
		return "ADDED";
	case IcebergManifestEntryStatusType::DELETED:
		return "DELETED";
	}
}

enum class IcebergManifestEntryContentType : uint8_t { DATA = 0, POSITION_DELETES = 1, EQUALITY_DELETES = 2 };

static string IcebergManifestEntryContentTypeToString(IcebergManifestEntryContentType type) {
	switch (type) {
	case IcebergManifestEntryContentType::DATA:
		return "EXISTING";
	case IcebergManifestEntryContentType::POSITION_DELETES:
		return "POSITION_DELETES";
	case IcebergManifestEntryContentType::EQUALITY_DELETES:
		return "EQUALITY_DELETES";
	}
}

//! The schema containing the fields from the manifest file.
//! this schema should match the generated cpp header from src/include/avro_codegen/iceberg_manifest_file_partial.hpp
static string MANIFEST_SCHEMA = "{\n"
                                "     \"type\": \"record\",\n"
                                "     \"name\": \"manifest_file\",\n"
                                "     \"fields\" : [\n"
                                "         {\"name\": \"manifest_path\", \"type\": \"string\"},\n"
                                "         {\"name\": \"content\", \"type\" : \"int\"},\n"
                                "         {\"name\": \"sequence_number\", \"type\" : \"long\"}\n"
                                "     ]\n"
                                " }";

// Schema for v1, sequence_number and content are not present there
static string MANIFEST_SCHEMA_V1 = "{\n"
                                "     \"type\": \"record\",\n"
                                "     \"name\": \"manifest_file\",\n"
                                "     \"fields\" : [\n"
                                "         {\"name\": \"manifest_path\", \"type\": \"string\"}\n"
                                "     ]\n"
                                " }";

//! An entry in the manifest list file (top level AVRO file)
struct IcebergManifest {
	//! Constructor from iceberg v2 spec manifest file
	explicit IcebergManifest(const c::manifest_file &schema) {
		manifest_path = schema.manifest_path;
		sequence_number = schema.sequence_number;
		content = (IcebergManifestContentType)schema.content;
	}

	//! Constructor from iceberg v1 spec manifest file
	explicit IcebergManifest(const c::manifest_file_v1 &schema) {
		manifest_path = schema.manifest_path;
		sequence_number = 0;
		content = IcebergManifestContentType::DATA;
	}

	//! Path to the manifest AVRO file
	string manifest_path;
	//! sequence_number when manifest was added to table (0 for Iceberg v1)
	int64_t sequence_number;
	//! either data or deletes
	IcebergManifestContentType content;

	void Print() {
		Printer::Print("  - Manifest = { content: " + IcebergManifestContentTypeToString(content) +
		               ", path: " + manifest_path + "}");
	}

	static vector<LogicalType> Types() {
		return {
		    LogicalType::VARCHAR,
		    LogicalType::BIGINT,
		    LogicalType::VARCHAR,
		};
	}

	static vector<string> Names() {
		return {"manifest_path", "manifest_sequence_number", "manifest_content"};
	}
};

//! The schema containing the fields from the manifest entry.
//! this schema should match the generated cpp header from src/include/avro_codegen/iceberg_manifest_entry_partial.hpp
// static string MANIFEST_ENTRY_SCHEMA = "{\n"
// "     \"type\": \"record\",\n"
// "     \"name\": \"manifest_entry\",\n"
// "     \"fields\" : [\n"
// "         {\"name\": \"status\", \"type\" : \"int\"},\n"
// "         {\"name\": \"data_file\", \"type\": {\n"
// "             \"type\": \"record\",\n"
// "             \"name\": \"r2\",\n"
// "             \"fields\" : [\n"
// "                 {\"name\": \"content\", \"type\": \"int\"},\n"
// "                 {\"name\": \"file_path\", \"type\": \"string\"},\n"
// "                 {\"name\": \"file_format\", \"type\": \"string\"},\n"
// "                 {\"name\": \"record_count\", \"type\" : \"long\"},\n"
// "                 {\"name\": \"lower_bounds\", \"type\": [\"null\", {\n"
// "                     \"type\": \"array\",\n"
// "                     \"items\": {\n"
// "                         \"type\": \"record\",\n"
// "                         \"name\": \"k126_v127\",\n"
// "                         \"fields\": [\n"
// "                             {\"name\": \"key\", \"type\": \"int\"},\n"
// "                             {\"name\": \"value\", \"type\": \"bytes\"}\n"
// "                         ]\n"
// "                     }\n"
// "                 }], \"default\": null},\n"
// "                 {\"name\": \"upper_bounds\", \"type\": [\"null\", {\n"
// "                     \"type\": \"array\",\n"
// "                     \"items\": {\n"
// "                         \"type\": \"record\",\n"
// "                         \"name\": \"k129_v130\",\n"
// "                         \"fields\": [\n"
// "                             {\"name\": \"key\", \"type\": \"int\"},\n"
// "                             {\"name\": \"value\", \"type\": \"bytes\"}\n"
// "                         ]\n"
// "                     }\n"
// "                 }], \"default\": null}\n"
// "           ]}\n"
// "         }\n"
// "     ]\n"
// " }";

static string MANIFEST_ENTRY_SCHEMA = R"(
{
  "type": "record",
  "name": "manifest_entry",
  "fields": [
    { "name": "status", "type": "int", "field-id": 0 },
    { "name": "data_file", "type": {
      "type": "record",
      "name": "r2",
      "fields": [
        { "name": "content", "type": "int", "field-id": 134 },
        { "name": "file_path", "type": "string", "field-id": 100 },
        { "name": "file_format", "type": "string", "field-id": 101 },
        { "name": "record_count", "type": "long", "field-id": 103 },
        { "name": "lower_bounds", "type": [
          "null",
          {
            "type": "array",
            "items": {
              "type": "record",
              "name": "k126_v127",
              "fields": [
                { "name": "key", "type": "int", "field-id": 126 },
                { "name": "value", "type": "bytes", "field-id": 127 }
              ]
            }
          }
        ], 
        "default": null,
        "field-id": 125 
        },
        { "name": "upper_bounds", "type": [
          "null",
          {
            "type": "array",
            "items": {
              "type": "record",
              "name": "k129_v130",
              "fields": [
                { "name": "key", "type": "int", "field-id": 129 },
                { "name": "value", "type": "bytes", "field-id": 130 }
              ]
            }
          }
        ], "default": null, "field-id": 128 }
      ]
    }, "field-id": 2 }
  ]
}
)";


static string MANIFEST_ENTRY_SCHEMA_V1 = "{\n"
                                      "     \"type\": \"record\",\n"
                                      "     \"name\": \"manifest_entry\",\n"
                                      "     \"fields\" : [\n"
                                      "         {\"name\": \"status\", \"type\" : \"int\"},\n"
                                      "         {\"name\": \"data_file\", \"type\": {\n"
                                      "             \"type\": \"record\",\n"
                                      "             \"name\": \"r2\",\n"
                                      "             \"fields\" : [\n"
                                      "                 {\"name\": \"file_path\", \"type\": \"string\"},\n"
                                      "                 {\"name\": \"file_format\", \"type\": \"string\"},\n"
                                      "                 {\"name\": \"record_count\", \"type\" : \"long\"}\n"
                                      "           ]}\n"
                                      "         }\n"
                                      "     ]\n"
                                      " }";


//! An entry in a manifest file
struct IcebergManifestEntry {
    explicit IcebergManifestEntry(const manifest_entry &schema) {
        status = (IcebergManifestEntryStatusType)schema.status;
        content = (IcebergManifestEntryContentType)schema.data_file.content;
        const auto &data_file = schema.data_file;
        file_path = data_file.file_path;
        file_format = data_file.file_format;
        record_count = data_file.record_count;
        lower_bounds.clear();
        upper_bounds.clear();

        // Handle lower_bounds
        if (data_file.lower_bounds.idx() == static_cast<size_t>(manifest_entry_json_Union__0__::Branch::array)) {
            const auto &bounds_array = data_file.lower_bounds.get_array();
            for (const auto &lb : bounds_array) {
                std::cout << "Lower bound key: " << lb.key << std::endl;
                lower_bounds[std::to_string(lb.key)] = DeserializeBound(lb.value);
            }
        } else {
            std::cout << "Lower bounds is null" << std::endl;
        }

        // Handle upper_bounds
        if (data_file.upper_bounds.idx() == static_cast<size_t>(manifest_entry_json_Union__1__::Branch::array)) {
            const auto &bounds_array = data_file.upper_bounds.get_array();
            for (const auto &ub : bounds_array) {
                upper_bounds[std::to_string(ub.key)] = DeserializeBound(ub.value);
            }
        } else {
            std::cout << "Upper bounds is null" << std::endl;
        }
    }

    explicit IcebergManifestEntry(const c::manifest_entry_v1 &schema) {
        status = (IcebergManifestEntryStatusType)schema.status;
        content = IcebergManifestEntryContentType::DATA;
        file_path = schema.data_file_.file_path;
        file_format = schema.data_file_.file_format;
        record_count = schema.data_file_.record_count;

        // Initialize bounds as empty maps
        lower_bounds.clear();
        upper_bounds.clear();
    }

    std::string DeserializeBound(const std::vector<uint8_t> &bound_value) {
        // Treat the byte vector as binary data and return a hexadecimal or Base64 representation
        std::ostringstream oss;
        for (auto byte : bound_value) {
            oss << std::hex << std::setw(2) << std::setfill('0') << (int)byte;
        }
        return oss.str();  // Return a hexadecimal string representation of the binary data
    }



    IcebergManifestEntryStatusType status;

    //! ----- Data File Struct ------
    IcebergManifestEntryContentType content;
    string file_path;
    string file_format;
    int64_t record_count;

    // Add new members for bounds
    std::unordered_map<string, Value> lower_bounds;
    std::unordered_map<string, Value> upper_bounds;

    void Print() {
        Printer::Print("    -> ManifestEntry = { type: " + IcebergManifestEntryStatusTypeToString(status) +
                       ", content: " + IcebergManifestEntryContentTypeToString(content) + ", file: " + file_path +
                       ", record_count: " + to_string(record_count) + "}");
    }

    static vector<LogicalType> Types() {
        return {
            LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
        };
    }

    static vector<string> Names() {
        return {"status", "content", "file_path", "file_format", "record_count"};
    }
};

struct IcebergTableEntry {
	IcebergManifest manifest;
	vector<IcebergManifestEntry> manifest_entries;

	void Print() {
		manifest.Print();
		for (auto &manifest_entry : manifest_entries) {
			manifest_entry.Print();
		}
	}
};
} // namespace duckdb
