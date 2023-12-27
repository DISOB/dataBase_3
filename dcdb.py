import duckdb


def setup(path: str): # подключается к базам данных
    conn = duckdb.connect(path)
    return conn


def cleanup(conn: duckdb.DuckDBPyConnection):
    conn.close()


def one_query(conn: duckdb.DuckDBPyConnection):
    conn.execute('SELECT PULocationID, count(*) FROM data GROUP BY 1')


def two_query(conn: duckdb.DuckDBPyConnection):
    conn.execute('SELECT passenger_count, avg(CAST(total_amount AS DOUBLE)) FROM data GROUP BY 1')


def three_query(conn: duckdb.DuckDBPyConnection):
    conn.execute("SELECT passenger_count, strftime('%Y', CAST(tpep_pickup_datetime AS DATE)), count(*) FROM data GROUP BY 1, 2")


def four_query(conn: duckdb.DuckDBPyConnection):
    conn.execute("SELECT passenger_count, strftime('%Y', CAST(tpep_pickup_datetime AS DATE)), round(CAST(trip_distance AS DOUBLE)) FROM data GROUP BY 1, 2, 3 ORDER BY 2, 3 DESC")