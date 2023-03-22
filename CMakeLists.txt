cmake_minimum_required(VERSION 2.8.12)

# Set extension name here
set(TARGET_NAME iceberg)
project(${TARGET_NAME})

# registers our custom findsnappy to match avro's find snappy behaviour
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CMAKE_CURRENT_SOURCE_DIR})

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
        ${YYJSON_OBJECT_FILES})

add_library(${EXTENSION_NAME} STATIC ${EXTENSION_SOURCES})

set(PARAMETERS "-warnings")
build_loadable_extension(${TARGET_NAME} ${PARAMETERS} ${EXTENSION_SOURCES})

### AVRO
include(ExternalProject)
ExternalProject_Add(avro_cpp_project
        GIT_REPOSITORY "https://github.com/apache/avro"
        GIT_TAG "release-1.11.1"
        GIT_CONFIG advice.detachedHead=false
        PREFIX "${CMAKE_CURRENT_BINARY_DIR}/avro-cpp-project"
        SOURCE_SUBDIR "lang/c++"
        CMAKE_CACHE_ARGS
        "-DBUILD_SHARED_LIBS:BOOL=OFF"
	"-DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=true"
	"-DCMAKE_INSTALL_PREFIX:PATH=${CMAKE_CURRENT_BINARY_DIR}/avro-cpp-project/install"
        #        "-DCMAKE_OSX_ARCHITECTURES:STRING=${CMAKE_OSX_ARCHITECTURES}"
        BUILD_BYPRODUCTS  "${CMAKE_CURRENT_BINARY_DIR}/avro-cpp-project/install/lib/${CMAKE_STATIC_LIBRARY_PREFIX}avrocpp_s${CMAKE_STATIC_LIBRARY_SUFFIX}"
        )
add_library(avro_cpp_static STATIC IMPORTED)
add_dependencies(avro_cpp_static avro_cpp_project)
set_property(TARGET avro_cpp_static PROPERTY IMPORTED_LOCATION "${CMAKE_CURRENT_BINARY_DIR}/avro-cpp-project/install/lib/${CMAKE_STATIC_LIBRARY_PREFIX}avrocpp_s${CMAKE_STATIC_LIBRARY_SUFFIX}")

target_link_libraries(${EXTENSION_NAME} avro_cpp_static)
target_link_libraries(${TARGET_NAME}_loadable_extension avro_cpp_static)
include_directories(${CMAKE_CURRENT_BINARY_DIR}/avro-cpp-project/install/include)
include_directories(${CMAKE_CURRENT_BINARY_DIR}/avro-cpp-project/src/avro_cpp_project/lang/c++/api) # TODO: why is this necessary?

### Dependencies for AVRO
# Boost
find_package(Boost REQUIRED COMPONENTS system iostreams)
target_link_libraries(${EXTENSION_NAME} ${Boost_LIBRARIES})
target_link_libraries(${TARGET_NAME}_loadable_extension ${Boost_LIBRARIES})
include_directories(${Boost_INCLUDE_DIRS})

# Snappy: If AVRO build finds snappy, it will link it. So we'll do the same here to ensure
find_package(Snappy)
if (SNAPPY_FOUND)
    target_link_libraries(${EXTENSION_NAME} ${SNAPPY_LIBRARIES})
    target_link_libraries(${TARGET_NAME}_loadable_extension ${SNAPPY_LIBRARIES})
endif()

install(
        TARGETS ${EXTENSION_NAME}
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
