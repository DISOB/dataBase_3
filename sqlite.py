import sqlite3


def setup(path):
    conn = sqlite3.connect(path)
    cursor = conn.cursor()
    return [conn, cursor]


def cleanup(conn: sqlite3.Connection, cursor: sqlite3.Cursor):
    cursor.close()
    conn.close()


def sqlt_one_query(cursor: sqlite3.Cursor):
    cursor.execute('SELECT PULocationID, count(*) FROM data GROUP BY 1')


def sqlt_two_query(cursor: sqlite3.Cursor):
    cursor.execute('SELECT passenger_count, avg(total_amount) FROM data GROUP BY 1')


def sqlt_three_query(cursor: sqlite3.Cursor):
    cursor.execute("SELECT passenger_count, strftime('%Y', tpep_pickup_datetime), count(*) FROM data GROUP BY 1, 2")


def sqlt_four_query(cursor: sqlite3.Cursor):
    cursor.execute("SELECT passenger_count, strftime('%Y', tpep_pickup_datetime), round(trip_distance) FROM data GROUP BY 1, 2, 3 ORDER BY 2, 3 DESC")