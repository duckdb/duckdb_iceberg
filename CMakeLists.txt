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
        src/common/iceberg.cpp
        src/iceberg_functions/iceberg_snapshots.cpp
        src/iceberg_functions/iceberg_scan.cpp
        src/iceberg_functions/iceberg_metadata.cpp
        src/yyjson/yyjson.cpp)

add_library(${EXTENSION_NAME} STATIC ${EXTENSION_SOURCES})

set(PARAMETERS "-warnings")
build_loadable_extension(${TARGET_NAME} ${PARAMETERS} ${EXTENSION_SOURCES})

# Get AVRO from vcpkg, note we're using an overlay port to force avro-cpp to version release-1.11.1
find_path(AVROCPP_INCLUDE_DIR avro/Encoder.hh)
find_library(AVROCPP_LIBRARY_DEBUG avrocpp_s PATH_SUFFIXES "debug/lib" REQUIRED)
get_filename_component(AVROCPP_ROOT_FIND_DIR ${AVROCPP_INCLUDE_DIR} DIRECTORY)
find_library(AVROCPP_LIBRARY_RELEASE avrocpp_s PATHS "${AVROCPP_ROOT_FIND_DIR}/lib/" REQUIRED NO_DEFAULT_PATH)
include_directories("${AVROCPP_INCLUDE_DIR}")

# NOTE: avro package doesn't support linking static libs even though they are in fact built. The workaround is this mess
# that will link the dependencies for us
set(SNAPPY_HOME "${AVROCPP_ROOT_FIND_DIR}")
set(Boost_USE_STATIC_LIBS ON)
find_package(Boost REQUIRED COMPONENTS iostreams program_options system filesystem)
find_package(Snappy CONFIG REQUIRED)
find_package(ZLIB REQUIRED)

set(AVRO_STATIC_DEPS Boost::boost Boost::iostreams Boost::program_options Boost::system Boost::filesystem Snappy::snappy ZLIB::ZLIB)

target_link_libraries(${EXTENSION_NAME} PUBLIC ${AVRO_STATIC_DEPS})
target_link_libraries(${EXTENSION_NAME} PUBLIC optimized "${AVROCPP_LIBRARY_RELEASE}" debug "${AVROCPP_LIBRARY_DEBUG}")
target_link_libraries(${TARGET_NAME}_loadable_extension ${AVRO_STATIC_DEPS})
target_link_libraries(${TARGET_NAME}_loadable_extension optimized "${AVROCPP_LIBRARY_RELEASE}" debug "${AVROCPP_LIBRARY_DEBUG}")

install(
        TARGETS ${EXTENSION_NAME}
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")