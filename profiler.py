import time
from stats import get_stats


# функция декоратор
def measure_n_times_with_stats(n: int):
    def inner_func(func):
        def wrapper(*args, **kwargs):
            data = []
            for _ in range(n):
                start = time.time()
                func(*args, **kwargs)
                end = time.time()
                res = (end-start)*1000
                data.append(res)
            [driver, query_type] = (str(func.__name__)).split('_')
            stats = get_stats(data, query_type, driver)
            return stats
        return wrapper
    return inner_func