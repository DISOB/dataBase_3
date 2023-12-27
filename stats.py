from typing import List # подсчёт статистики
import pandas as pd

driver_k = 'driver'
type_k = 'query_type'
min_k = 'min'
max_k = 'max'
mean_k = 'mean'


def get_stats(data: List[float], query_type: str, driver: str): # подсчитывает максимальное и медлианное время выполнения .
    data.sort()
    min_ = data[0]
    max_ = data[len(data) - 1]
    mean = data[len(data) // 2]
    stats = {
        driver_k: driver,
        type_k: query_type,
        min_k: min_,
        max_k: max_,
        mean_k: mean
    }
    return stats


def init_dataframe():
    return pd.DataFrame(columns=['driver', 'query_type', 'min', 'max', 'mean'])


def save_2_file(df: pd.DataFrame, path: str):
    df.to_csv(index=False, path_or_buf=path)