#include "duckdb.hpp"
#include "iceberg_utils.hpp"
#include "zlib.h"
#include "fstream"

namespace duckdb {

string IcebergUtils::FileToString(const string &path, FileSystem &fs) {
	auto handle =
	    fs.OpenFile(path, FileFlags::FILE_FLAGS_READ, FileSystem::DEFAULT_LOCK, FileSystem::DEFAULT_COMPRESSION);
	auto file_size = handle->GetFileSize();
	string ret_val(file_size, ' ');
	handle->Read((char *)ret_val.c_str(), file_size);
	return ret_val;
}

// Function to decompress a gz file content string
string IcebergUtils::GzFileToString(const string &path, FileSystem &fs) {
  // Initialize zlib variables
  string gzipped_string = FileToString(path, fs);
  std::stringstream decompressed;
    int CHUNK_SIZE = 16384;
    z_stream zs;
    zs.zalloc = Z_NULL;
    zs.zfree = Z_NULL;
    zs.opaque = Z_NULL;
    zs.avail_in = gzipped_string.size();
    zs.next_in = (Bytef *)gzipped_string.data();
    int ret = inflateInit2(&zs, 16 + MAX_WBITS); // MAX_WBITS + 16 to enable gzip decoding
    if (ret != Z_OK)
    {
        throw std::runtime_error("inflateInit failed");
    }
    do
    {
        char out[CHUNK_SIZE];
        zs.avail_out = CHUNK_SIZE;
        zs.next_out = (Bytef *)out;
        ret = inflate(&zs, Z_NO_FLUSH);
        if (ret < 0)
        {
            inflateEnd(&zs);
            throw std::runtime_error("inflate failed with error code " + to_string(ret));
        }
        decompressed.write(out, CHUNK_SIZE - zs.avail_out);
    } while (zs.avail_out == 0);
    inflateEnd(&zs);
    string ds = decompressed.str();
    return ds;
}

string IcebergUtils::GetFullPath(const string &iceberg_path, const string &relative_file_path, FileSystem &fs) {
	std::size_t found = relative_file_path.find("/metadata/");
	if (found != string::npos) {
		return fs.JoinPath(iceberg_path, relative_file_path.substr(found + 1));
	}

	found = relative_file_path.find("/data/");
	if (found != string::npos) {
		return fs.JoinPath(iceberg_path, relative_file_path.substr(found + 1));
	}

	throw IOException("Did not recognize iceberg path");
}

uint64_t IcebergUtils::TryGetNumFromObject(yyjson_val *obj, const string &field) {
	auto val = yyjson_obj_getn(obj, field.c_str(), field.size());
	if (!val || yyjson_get_tag(val) != YYJSON_TYPE_NUM) {
		throw IOException("Invalid field found while parsing field: " + field);
	}
	return yyjson_get_uint(val);
}

bool IcebergUtils::TryGetBoolFromObject(yyjson_val *obj, const string &field) {
	auto val = yyjson_obj_getn(obj, field.c_str(), field.size());
	if (!val || yyjson_get_tag(val) != YYJSON_TYPE_BOOL) {
		throw IOException("Invalid field found while parsing field: " + field);
	}
	return yyjson_get_bool(val);
}

string IcebergUtils::TryGetStrFromObject(yyjson_val *obj, const string &field) {
	auto val = yyjson_obj_getn(obj, field.c_str(), field.size());
	if (!val || yyjson_get_tag(val) != YYJSON_TYPE_STR) {
		throw IOException("Invalid field found while parsing field: " + field);
	}
	return yyjson_get_str(val);
}

} // namespace duckdb