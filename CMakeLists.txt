cmake_minimum_required(VERSION 2.8.12)

# Set extension name here
set(TARGET_NAME iceberg)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
project(${TARGET_NAME})
include_directories(src/include)

include_directories(duckdb/extension/json/yyjson/include)
include_directories(duckdb/extension/json/include)

set(EXTENSION_SOURCES
        src/iceberg_extension.cpp
        src/iceberg_functions.cpp
        src/iceberg_functions/iceberg_snapshots.cpp
        src/iceberg_functions/iceberg_scan.cpp
        ${YYJSON_OBJECT_FILES})

add_library(${EXTENSION_NAME} STATIC ${EXTENSION_SOURCES})

set(PARAMETERS "-warnings")
build_loadable_extension(${TARGET_NAME} ${PARAMETERS} ${EXTENSION_SOURCES})

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
