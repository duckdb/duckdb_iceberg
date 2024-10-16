string IcebergUtils::GetFullPath(const string &base_path, const string &relative_path, FileSystem &fs) {
    if (fs.IsAbsolutePath(relative_path)) {
        return relative_path;
    }
    return fs.JoinPath(base_path, relative_path);
}