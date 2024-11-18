
set -ex

docker-compose kill
docker-compose rm -f
docker-compose up -d
docker-compose logs -f mc

pip3 install -r requirements.txt

python3 provision.py

# Would be nice to have rest support in there :)
UNPARTITIONED_TABLE_PATH=$(curl -s http://127.0.0.1:8181/v1/namespaces/default/tables/table_unpartitioned | jq -r '."metadata-location"')

SQL=$(cat <<-END

CREATE SECRET (
  TYPE S3,
  KEY_ID 'admin',
  SECRET 'password',
  ENDPOINT '127.0.0.1:9000',
  URL_STYLE 'path',
  USE_SSL 0
);

SELECT * FROM iceberg_scan('${UNPARTITIONED_TABLE_PATH}');
END

)

if test -f "../build/release/duckdb"
then
  # in CI
  ../build/release/duckdb -s "$SQL"
else
  duckdb -s "$SQL"
fi
