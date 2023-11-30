cmake_minimum_required(VERSION 2.8.12)

# Set extension name here
set(TARGET_NAME iceberg)
project(${TARGET_NAME})

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED True)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
include_directories(src/include)

set(EXTENSION_SOURCES
    src/iceberg_extension.cpp
    src/iceberg_functions.cpp
    src/common/utils.cpp
    src/common/schema.cpp
    src/common/iceberg.cpp
    src/iceberg_functions/iceberg_snapshots.cpp
    src/iceberg_functions/iceberg_scan.cpp
    src/iceberg_functions/iceberg_metadata.cpp
    src/yyjson/yyjson.cpp)

add_library(${EXTENSION_NAME} STATIC ${EXTENSION_SOURCES})

set(PARAMETERS "-warnings")
build_loadable_extension(${TARGET_NAME} ${PARAMETERS} ${EXTENSION_SOURCES})

# Get AVRO from vcpkg, note we're using an overlay port to force avro-cpp to
# version release-1.11.1. Also we deviate from the hinted usage, as that only
# seems to support dynamic linking.
find_path(AVROCPP_INCLUDE_DIR avro/Encoder.hh)
find_library(AVROCPP_LIBRARY_DEBUG avrocpp_s PATH_SUFFIXES "debug/lib" REQUIRED)
get_filename_component(AVROCPP_ROOT_FIND_DIR ${AVROCPP_INCLUDE_DIR} DIRECTORY)
find_library(
  AVROCPP_LIBRARY_RELEASE avrocpp_s
  PATHS "${AVROCPP_ROOT_FIND_DIR}/lib/" REQUIRED
  NO_DEFAULT_PATH)
add_library(avro_static_release STATIC IMPORTED)
set_target_properties(
  avro_static_release
  PROPERTIES IMPORTED_LOCATION ${AVROCPP_LIBRARY_RELEASE}
             INTERFACE_INCLUDE_DIRECTORIES ${AVROCPP_INCLUDE_DIR})
add_library(avro_static_debug STATIC IMPORTED)
set_target_properties(
  avro_static_debug
  PROPERTIES IMPORTED_LOCATION ${AVROCPP_LIBRARY_DEBUG}
             INTERFACE_INCLUDE_DIRECTORIES ${AVROCPP_INCLUDE_DIR})

# Note: for some reason avro-cpp port does properly handle static deps, so we
# need to manually ensure avro's deps are linked too
set(SNAPPY_HOME "${AVROCPP_ROOT_FIND_DIR}")
set(Boost_USE_STATIC_LIBS ON)
find_package(Boost REQUIRED COMPONENTS iostreams program_options system
                                       filesystem)
find_package(Snappy CONFIG REQUIRED)
find_package(ZLIB REQUIRED)
target_link_libraries(
  avro_static_release
  INTERFACE Boost::boost
            Boost::iostreams
            Boost::program_options
            Boost::system
            Boost::filesystem
            Snappy::snappy
            ZLIB::ZLIB)
target_link_libraries(
  avro_static_debug
  INTERFACE Boost::boost
            Boost::iostreams
            Boost::program_options
            Boost::system
            Boost::filesystem
            Snappy::snappy
            ZLIB::ZLIB)

# Link dependencies into extension
target_link_libraries(${EXTENSION_NAME} PUBLIC optimized avro_static_release
                                               debug avro_static_debug)
target_link_libraries(${TARGET_NAME}_loadable_extension optimized
                      avro_static_release debug avro_static_debug)

install(
  TARGETS ${EXTENSION_NAME} ${TARGET_NAME}_loadable_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
