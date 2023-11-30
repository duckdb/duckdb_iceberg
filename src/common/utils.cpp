#include "duckdb.hpp"
#include "iceberg_utils.hpp"

namespace duckdb {

string IcebergUtils::FileToString(const string &path, FileSystem &fs) {
	auto handle =
	    fs.OpenFile(path, FileFlags::FILE_FLAGS_READ, FileSystem::DEFAULT_LOCK, FileSystem::DEFAULT_COMPRESSION);
	auto file_size = handle->GetFileSize();
	string ret_val(file_size, ' ');
	handle->Read((char *)ret_val.c_str(), file_size);
	return ret_val;
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