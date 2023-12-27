from sqlite3 import Cursor
import sqlt.sqlite as sqlt
from profiler import measure_n_times_with_stats
from const import measure_times


@measure_n_times_with_stats(measure_times)
def sqlite_1(c: Cursor):
    sqlt.sqlt_one_query(c)


@measure_n_times_with_stats(measure_times)
def sqlite_2(c: Cursor):
    sqlt.sqlt_two_query(c)


@measure_n_times_with_stats(measure_times)
def sqlite_3(c: Cursor):
    sqlt.sqlt_three_query(c)


@measure_n_times_with_stats(measure_times)
def sqlite_4(c: Cursor):
    sqlt.sqlt_four_query(c)