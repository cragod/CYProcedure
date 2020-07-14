from multiprocessing.pool import Pool
from datetime import datetime
import numpy as np
import time


class MultiTest:
    n = 0

    def print_something(self, param):
        print(self.n)
        self.n += 1
        return np.math.factorial(param)

    def run_task(self):
        start_time = datetime.now()
        params = np.arange(0, 10000)
        with Pool(processes=4) as pool:
            res = pool.map(self.print_something, params)
            print(datetime.now() - start_time)


MultiTest().run_task()
