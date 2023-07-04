cmake_minimum_required(VERSION 2.8.12)

# Set extension name here
set(TARGET_NAME iceberg)
project(${TARGET_NAME})

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED True)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
include_directories(src/include)
include_directories(duckdb/extension/json/yyjson/include)
include_directories(duckdb/extension/json/include)

set(EXTENSION_SOURCES
        src/iceberg_extension.cpp
        src/iceberg_functions.cpp
        src/common/utils.cpp
        src/common/iceberg.cpp
        src/iceberg_functions/iceberg_snapshots.cpp
        src/iceberg_functions/iceberg_scan.cpp
        src/iceberg_functions/iceberg_metadata.cpp
        ${YYJSON_OBJECT_FILES})

add_library(${EXTENSION_NAME} STATIC ${EXTENSION_SOURCES})

set(PARAMETERS "-warnings")
build_loadable_extension(${TARGET_NAME} ${PARAMETERS} ${EXTENSION_SOURCES})

### AVRO
# TODO: unfortunately, vcpkg's version of Avro-CPP does not work with iceberg spec. It doesnt properly handle non-string
#       custom attributes in this line: https://github.com/apache/avro/blob/e44b680621328c4e6524bd2983af1ce11afeebed/lang/c%2B%2B/impl/Compiler.cc#L278
#       when it does, we can switch back to the much simpler vcpkg based build
# TODO2: PR the patched version to vcpkg, then switch back to
include(ExternalProject)
ExternalProject_Add(avro_cpp_project
        GIT_REPOSITORY "https://github.com/samansmink/avro"
        GIT_TAG "12379c9760b38a97668d54697a640a162aede1ee"
        GIT_CONFIG advice.detachedHead=false
        PREFIX "${CMAKE_CURRENT_BINARY_DIR}/avro-cpp-project"
        SOURCE_SUBDIR "lang/c++"
        CMAKE_CACHE_ARGS
        "-DBUILD_SHARED_LIBS:BOOL=OFF"
        "-DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=true"
        "-DCMAKE_INSTALL_PREFIX:PATH=${CMAKE_CURRENT_BINARY_DIR}/avro-cpp-project/install"
        "-DCMAKE_OSX_ARCHITECTURES:STRING=${OSX_BUILD_ARCH}"
        "-DCMAKE_TOOLCHAIN_FILE:STRING=${CMAKE_TOOLCHAIN_FILE}"
        "-DVCPKG_TRACE_FIND_PACKAGE:BOOL=true"
        "-DCMAKE_CXX_STANDARD:STRING=14"
        "-DCMAKE_CXX_STANDARD_REQUIRED:BOOL=true"
        "-DVCPKG_TARGET_TRIPLET:STRING=${VCPKG_TARGET_TRIPLET}"
        BUILD_BYPRODUCTS  "${CMAKE_CURRENT_BINARY_DIR}/avro-cpp-project/install/lib/${CMAKE_STATIC_LIBRARY_PREFIX}avrocpp_s${CMAKE_STATIC_LIBRARY_SUFFIX}"
        )
add_library(avro_cpp_static STATIC IMPORTED)
add_dependencies(avro_cpp_static avro_cpp_project)
set_property(TARGET avro_cpp_static PROPERTY IMPORTED_LOCATION "${CMAKE_CURRENT_BINARY_DIR}/avro-cpp-project/install/lib/${CMAKE_STATIC_LIBRARY_PREFIX}avrocpp_s${CMAKE_STATIC_LIBRARY_SUFFIX}")

# Get from vcpkg: BOOST <3 (avro-cpp dependency)
set(Boost_USE_STATIC_LIBS ON)
find_package(Boost REQUIRED COMPONENTS iostreams program_options system filesystem)
target_link_libraries(avro_cpp_static INTERFACE Boost::boost Boost::iostreams Boost::program_options Boost::system Boost::filesystem)
include_directories(${Boost_INCLUDE_DIRS})

# Get from vcpkg: Schni Schna Schnappi (avro-cpp dependency)
find_package(Snappy CONFIG REQUIRED)
target_link_libraries(avro_cpp_static INTERFACE Snappy::snappy)

# Get from vcpkg: ZLIB (avro-cpp dependency)
find_package(ZLIB REQUIRED)
target_link_libraries(avro_cpp_static INTERFACE ZLIB::ZLIB)

target_link_libraries(${EXTENSION_NAME} avro_cpp_static)
target_link_libraries(${TARGET_NAME}_loadable_extension avro_cpp_static)
include_directories(${CMAKE_CURRENT_BINARY_DIR}/avro-cpp-project/install/include)
include_directories(${CMAKE_CURRENT_BINARY_DIR}/avro-cpp-project/src/avro_cpp_project/lang/c++/api) # TODO: why is this necessary?

# TODO re-enable when avro is updated and the vcppkg latest works again
#find_path(AVROCPP_INCLUDE_DIR avro/Encoder.hh)
#find_library(AVROCPP_LIBRARY_DEBUG avrocpp PATH_SUFFIXES "debug/lib" REQUIRED)
#get_filename_component(AVROCPP_ROOT_FIND_DIR ${AVROCPP_INCLUDE_DIR} DIRECTORY)
#find_library(AVROCPP_LIBRARY_RELEASE avrocpp PATHS "${AVROCPP_ROOT_FIND_DIR}/lib/" REQUIRED NO_DEFAULT_PATH)
#include_directories("${AVROCPP_INCLUDE_DIR}")
#target_link_libraries(${EXTENSION_NAME} PRIVATE optimized "${AVROCPP_LIBRARY_RELEASE}" debug "${AVROCPP_LIBRARY_DEBUG}")

install(
        TARGETS ${EXTENSION_NAME}
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")