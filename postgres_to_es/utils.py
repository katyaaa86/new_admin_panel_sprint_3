import logging
from functools import wraps
from time import sleep


class TransferDataError(Exception):
    pass


def backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10):
    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            sleep_time = 0
            result = None

            while sleep_time <= border_sleep_time:
                try:
                    sleep(sleep_time)
                    result = func(*args, *kwargs)
                    break
                except Exception as error:
                    logging.error(f'Backoff error: {str(error)}')
                    if sleep_time == 0:
                        sleep_time = start_sleep_time
                    else:
                        sleep_time = sleep_time * 2 ** factor if sleep_time < border_sleep_time else border_sleep_time
            else:
                raise TransferDataError
            return result
        return inner
    return func_wrapper
