.PHONY: all clean format debug release duckdb_debug duckdb_release pull update

all: release

MKFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
PROJ_DIR := $(dir $(MKFILE_PATH))
DISABLE_SANITIZER_FLAG ?=

OSX_BUILD_UNIVERSAL_FLAG=
ifneq (${OSX_BUILD_ARCH}, "")
	OSX_BUILD_UNIVERSAL_FLAG=-DOSX_BUILD_ARCH=${OSX_BUILD_ARCH}
endif
ifeq (${STATIC_LIBCPP}, 1)
	STATIC_LIBCPP=-DSTATIC_LIBCPP=TRUE
endif

ifeq (${DISABLE_SANITIZER}, 1)
	DISABLE_SANITIZER_FLAG=-DENABLE_SANITIZER=FALSE -DENABLE_UBSAN=0
endif

VCPKG_TOOLCHAIN_PATH?=
ifneq ("${VCPKG_TOOLCHAIN_PATH}", "")
	TOOLCHAIN_FLAGS:=${TOOLCHAIN_FLAGS} -DVCPKG_MANIFEST_DIR='${PROJ_DIR}' -DVCPKG_BUILD=1 -DCMAKE_TOOLCHAIN_FILE='${VCPKG_TOOLCHAIN_PATH}'
endif
ifneq ("${VCPKG_TARGET_TRIPLET}", "")
	TOOLCHAIN_FLAGS:=${TOOLCHAIN_FLAGS} -DVCPKG_TARGET_TRIPLET='${VCPKG_TARGET_TRIPLET}'
endif

ifeq ($(GEN),ninja)
	GENERATOR=-G "Ninja"
	FORCE_COLOR=-DFORCE_COLORED_OUTPUT=1
endif

BUILD_FLAGS=-DEXTENSION_STATIC_BUILD=1 -DBUILD_EXTENSIONS="httpfs" ${OSX_BUILD_UNIVERSAL_FLAG} ${STATIC_LIBCPP} ${TOOLCHAIN_FLAGS}

CLIENT_FLAGS :=

# These flags will make DuckDB build the extension
EXTENSION_FLAGS=-DDUCKDB_EXTENSION_NAMES="iceberg" -DDUCKDB_EXTENSION_ICEBERG_PATH="$(PROJ_DIR)" -DDUCKDB_EXTENSION_ICEBERG_SHOULD_LINK=1 -DDUCKDB_EXTENSION_ICEBERG_INCLUDE_PATH="$(PROJ_DIR)src/include"

pull:
	git submodule init
	git submodule update --recursive --remote

clean:
	rm -rf build
	rm -rf testext
	cd duckdb && make clean

# Main build
debug:
	mkdir -p  build/debug && \
	cmake $(GENERATOR) $(FORCE_COLOR) $(EXTENSION_FLAGS) ${CLIENT_FLAGS} ${DISABLE_SANITIZER_FLAG} -DEXTENSION_STATIC_BUILD=1 -DCMAKE_BUILD_TYPE=Debug ${BUILD_FLAGS} -S ./duckdb/ -B build/debug && \
	cmake --build build/debug --config Debug

release:
	mkdir -p build/release && \
	cmake $(GENERATOR) $(FORCE_COLOR) $(EXTENSION_FLAGS) ${CLIENT_FLAGS} ${DISABLE_SANITIZER_FLAG} -DEXTENSION_STATIC_BUILD=1 -DCMAKE_BUILD_TYPE=Release ${BUILD_FLAGS} -S ./duckdb/ -B build/release && \
	cmake --build build/release --config Release

reldebug:
	mkdir -p build/release && \
	cmake $(GENERATOR) $(FORCE_COLOR) $(EXTENSION_FLAGS) ${CLIENT_FLAGS} ${DISABLE_SANITIZER_FLAG} -DEXTENSION_STATIC_BUILD=1 -DCMAKE_BUILD_TYPE=RelWithDebInfo ${BUILD_FLAGS} -S ./duckdb/ -B build/reldebug && \
	cmake --build build/release --config RelWithDebInfo

# Client build
debug_js: CLIENT_FLAGS=-DBUILD_NODE=1
debug_js: debug

debug_r: CLIENT_FLAGS=-DBUILD_R=1
debug_r: debug

debug_python: CLIENT_FLAGS=-DBUILD_PYTHON=1 -DBUILD_EXTENSIONS=fts;tpch;visualizer;tpcds
debug_python: debug

release_js: CLIENT_FLAGS=-DBUILD_NODE=1
release_js: release

release_r: CLIENT_FLAGS=-DBUILD_R=1
release_r: release

release_python: CLIENT_FLAGS=-DBUILD_PYTHON=1 -DBUILD_EXTENSIONS=fts;tpch;visualizer;tpcds
release_python: release

# Main tests
test: test_release

test_release: release
	./build/release/test/unittest --test-dir . "[sql]"

test_debug: debug
	./build/debug/test/unittest --test-dir . "[sql]"

# Client tests
test_js: test_debug_js
test_debug_js: debug_js
	cd duckdb/tools/nodejs && npm run test-path -- "../../../test/nodejs/**/*.js"

test_release_js: release_js
	cd duckdb/tools/nodejs && npm run test-path -- "../../../test/nodejs/**/*.js"

test_python: test_debug_python
test_debug_python: debug_python
	cd test/python && python3 -m pytest

test_release_python: release_python
	cd test/python && python3 -m pytest

format:
	find src/ -iname *.hpp -o -iname *.cpp | xargs clang-format --sort-includes=0 -style=file -i
	cmake-format -i CMakeLists.txt

update:
	git submodule update --remote --merge

data: data_clean
	python3 scripts/test_data_generator/generate_iceberg.py 0.01 data/iceberg/generated_spec1_0_01 1
	python3 scripts/test_data_generator/generate_iceberg.py 0.01 data/iceberg/generated_spec2_0_01 2

data_large: data data_clean
	python3 scripts/test_data_generator/generate_iceberg.py 1 data/iceberg/generated_spec2_1 2

data_clean:
	rm -rf data/iceberg/generated_*