import time


class CallTime:

    @staticmethod
    def call_time(func):
        def timer(*args):
            start = time.time()
            result = func(*args)
            end = time.time()
            print(func.__name__, 'took', str(end - start) * 1000, 'ms')
            return result
        return timer