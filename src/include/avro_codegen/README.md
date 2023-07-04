# AVRO CPP generated headers
These files are generated using the AVRO C++ library codegen util. Check out https://avro.apache.org/docs/1.11.1/api/cpp/html/#GettingStarted for details.

# Difference between full and partial headers
Full contains the whole schema of the iceberg spec, partial reads just the fields
that are currently used in this extension. The extension contains both for developer
convenience but partial should probably be used.