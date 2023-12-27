from stats import init_dataframe, save_2_file
import os
from dotenv import load_dotenv
import sqlite as sqlt
import bench_sqlt as bench_sqlt
import psycopg as psy
import bench_psy
import dcdb as duck
import bench_duck


def get_postgres_data(): # создание строковых значений для передачи
    return {
        'dbname': os.environ.get('DB_NAME'),
        'user': os.environ.get('DB_USER'),
        'password': os.environ.get('DB_PASSWORD'),
        'host': os.environ.get('DB_HOST'),
        'port': os.environ.get('DB_PORT')
    }


def load_env(): # загрузка переменного окружения
    dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
    if os.path.exists(dotenv_path):
        load_dotenv(dotenv_path)
    else:
        raise Exception('No .env file')


# times_to_execute = 10

if __name__ == '__main__':
    load_env()
    df = init_dataframe()
    csv_path = os.environ.get('CSV_PATH')
    db_params = get_postgres_data()
    sqlite_path = os.environ.get('SQLT_DATA_SOURCE')
    duck_path = os.environ.get('DUCK_DATA_SOURCE')
    stats = []

    [conn_psy, curs_psy] = psy.setup(db_params) # получение переменных для общения с базой данных
    [conn_sqlt, curs_sqlt] = sqlt.setup(sqlite_path)
    conn_duck = duck.setup(duck_path)

    def save_data(df, data):
        for d in data:
            df.loc[len(df)] = d
        save_2_file(df, csv_path)

    def benchmarker(b_psy, b_sqlt, b_d, st):
        st.append(b_psy(curs_psy))
        print('Done psycopg')
        st.append(b_sqlt(curs_sqlt))
        print('Done sqlite')
        st.append(b_d(conn_duck))
        print('Done duckdb')
        return st


    stats = benchmarker(bench_psy.psycopg2_1, bench_sqlt.sqlite_1, bench_duck.duckdb_1, stats)
    save_data(df, stats)
    stats = benchmarker(bench_psy.psycopg2_2, bench_sqlt.sqlite_2, bench_duck.duckdb_2, stats)
    save_data(df, stats)
    stats = benchmarker(bench_psy.psycopg2_3, bench_sqlt.sqlite_3, bench_duck.duckdb_3, stats)
    save_data(df, stats)
    stats = benchmarker(bench_psy.psycopg2_4, bench_sqlt.sqlite_4, bench_duck.duckdb_4, stats)
    save_data(df, stats)

    duck.cleanup(conn_duck)
    sqlt.cleanup(conn_sqlt, curs_sqlt)
    psy.cleanup(conn_psy, curs_psy)