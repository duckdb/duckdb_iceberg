PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=iceberg
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

# Custom makefile targets
data: data_clean
	python3 scripts/test_data_generator/generate_iceberg.py 0.01 data/iceberg/generated_spec1_0_01 1
	python3 scripts/test_data_generator/generate_iceberg.py 0.01 data/iceberg/generated_spec2_0_01 2

data_large: data data_clean
	python3 scripts/test_data_generator/generate_iceberg.py 1 data/iceberg/generated_spec2_1 2

data_clean:
	rm -rf data/iceberg/generated_*