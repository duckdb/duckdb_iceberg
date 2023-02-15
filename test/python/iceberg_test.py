import duckdb

def test_iceberg():
    conn = duckdb.connect('');
    conn.execute("SELECT iceberg('Sam') as value;");
    res = conn.fetchall()
    assert(res[0][0] == "Iceberg Sam 🐥");