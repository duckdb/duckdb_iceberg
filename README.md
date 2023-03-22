# DuckDB Iceberg extension
Extension for DuckDB to read from Apache Iceberg.

# Dependencies
Currently building the extension requires Boost. Additionally, if the extension was built on a system with Snappy, it also requires
snappy to run the extension.

To install on linux:
```sh
apt-get install libsnappy-dev libboost-dev
```

To install on MacOS:
```sh
brew install boost
brew install snappy
```