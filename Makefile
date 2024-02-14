.PHONY: all clean format debug release duckdb_debug duckdb_release pull update wasm_mvp wasm_eh wasm_threads

all: release

MKFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
PROJ_DIR := $(dir $(MKFILE_PATH))
DISABLE_SANITIZER_FLAG ?=

ifeq ($(OS),Windows_NT)
	TEST_PATH="/test/Release/unittest.exe"
else
	TEST_PATH="/test/unittest"
endif

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
#### VCPKG config
VCPKG_TOOLCHAIN_PATH?=
ifneq ("${VCPKG_TOOLCHAIN_PATH}", "")
	TOOLCHAIN_FLAGS:=${TOOLCHAIN_FLAGS} -DVCPKG_MANIFEST_DIR='${PROJ_DIR}' -DVCPKG_BUILD=1 -DCMAKE_TOOLCHAIN_FILE='${VCPKG_TOOLCHAIN_PATH}'
endif
ifneq ("${VCPKG_TARGET_TRIPLET}", "")
	TOOLCHAIN_FLAGS:=${TOOLCHAIN_FLAGS} -DVCPKG_TARGET_TRIPLET='${VCPKG_TARGET_TRIPLET}'
endif

#### Enable Ninja as generator
ifeq ($(GEN),ninja)
	GENERATOR=-G "Ninja"
	FORCE_COLOR=-DFORCE_COLORED_OUTPUT=1
endif

BUILD_FLAGS=-DEXTENSION_STATIC_BUILD=1 -DBUILD_EXTENSIONS="httpfs" ${OSX_BUILD_UNIVERSAL_FLAG} ${STATIC_LIBCPP} ${TOOLCHAIN_FLAGS} -DDUCKDB_EXPLICIT_PLATFORM='${DUCKDB_PLATFORM}'

EXT_NAME=iceberg

#### Configuration for this extension
EXTENSION_NAME=ICEBERG
EXTENSION_FLAGS=\
-DDUCKDB_EXTENSION_NAMES="iceberg" \
-DDUCKDB_EXTENSION_${EXTENSION_NAME}_PATH="$(PROJ_DIR)" \
-DDUCKDB_EXTENSION_${EXTENSION_NAME}_LOAD_TESTS=1 \
-DDUCKDB_EXTENSION_${EXTENSION_NAME}_SHOULD_LINK=1 \
-DDUCKDB_EXTENSION_${EXTENSION_NAME}_INCLUDE_PATH="$(PROJ_DIR)src/include" \
-DDUCKDB_EXTENSION_${EXTENSION_NAME}_TEST_PATH="$(PROJ_DIR)test/sql"


CLIENT_FLAGS :=

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
	./build/release/$(TEST_PATH) "$(PROJ_DIR)test/*"
test_debug: debug
	./build/debug/$(TEST_PATH) "$(PROJ_DIR)test/*"

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

WASM_LINK_TIME_FLAGS=

wasm_mvp:
	mkdir -p build/wasm_mvp
	emcmake cmake $(GENERATOR) -DWASM_LOADABLE_EXTENSIONS=1 -DBUILD_EXTENSIONS_ONLY=1 -Bbuild/wasm_mvp -DCMAKE_CXX_FLAGS="-DDUCKDB_CUSTOM_PLATFORM=wasm_mvp" -DSKIP_EXTENSIONS="parquet" -S duckdb $(TOOLCHAIN_FLAGS) $(EXTENSION_FLAGS) -DVCPKG_CHAINLOAD_TOOLCHAIN_FILE=$(EMSDK)/upstream/emscripten/cmake/Modules/Platform/Emscripten.cmake
	emmake make -j8 -Cbuild/wasm_mvp
	cd build/wasm_mvp/extension/${EXT_NAME} && emcc $f -sSIDE_MODULE=1 -o ../../${EXT_NAME}.duckdb_extension.wasm -O3 ${EXT_NAME}.duckdb_extension $(WASM_LINK_TIME_FLAGS)

wasm_eh:
	mkdir -p build/wasm_eh
	emcmake cmake $(GENERATOR) -DWASM_LOADABLE_EXTENSIONS=1 -DBUILD_EXTENSIONS_ONLY=1 -Bbuild/wasm_eh -DCMAKE_CXX_FLAGS="-fwasm-exceptions -DWEBDB_FAST_EXCEPTIONS=1 -DDUCKDB_CUSTOM_PLATFORM=wasm_eh" -DSKIP_EXTENSIONS="parquet" -S duckdb $(TOOLCHAIN_FLAGS) $(EXTENSION_FLAGS) -DVCPKG_CHAINLOAD_TOOLCHAIN_FILE=$(EMSDK)/upstream/emscripten/cmake/Modules/Platform/Emscripten.cmake
	emmake make -j8 -Cbuild/wasm_eh
	cd build/wasm_eh/extension/${EXT_NAME} && emcc $f -sSIDE_MODULE=1 -o ../../${EXT_NAME}.duckdb_extension.wasm -O3 ${EXT_NAME}.duckdb_extension $(WASM_LINK_TIME_FLAGS)

wasm_threads:
	mkdir -p ./build/wasm_threads
	emcmake cmake $(GENERATOR) -DWASM_LOADABLE_EXTENSIONS=1 -DBUILD_EXTENSIONS_ONLY=1 -Bbuild/wasm_threads -DCMAKE_CXX_FLAGS="-fwasm-exceptions -DWEBDB_FAST_EXCEPTIONS=1 -DWITH_WASM_THREADS=1 -DWITH_WASM_SIMD=1 -DWITH_WASM_BULK_MEMORY=1 -DDUCKDB_CUSTOM_PLATFORM=wasm_threads" -DSKIP_EXTENSIONS="parquet" -S duckdb $(TOOLCHAIN_FLAGS) $(EXTENSION_FLAGS) -DVCPKG_CHAINLOAD_TOOLCHAIN_FILE=$(EMSDK)/upstream/emscripten/cmake/Modules/Platform/Emscripten.cmake
	emmake make -j8 -Cbuild/wasm_threads
	cd build/wasm_threads/extension/${EXT_NAME} && emcc $f -sSIDE_MODULE=1 -o ../../${EXT_NAME}.duckdb_extension.wasm -O3 ${EXT_NAME}.duckdb_extension $(WASM_LINK_TIME_FLAGS)
