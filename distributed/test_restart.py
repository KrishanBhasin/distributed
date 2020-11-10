import dask
import distributed
import numpy as np
import time

if __name__=="__main__":

    cluster = distributed.LocalCluster(n_workers=2, threads_per_worker=1, memory_limit="512M")
    client = distributed.Client(cluster)

    x = {}
    def memory_leaking_fn(data, sleep=0):
        x[data] = np.random.randint(100, size=12 * 1024**2 // 8)
        
        time.sleep(sleep)
        return data

    futures = client.map(memory_leaking_fn, range(1000))
    futures = client.map(memory_leaking_fn, range(1000), np.repeat(0.1, 1000))

    for f in futures:
        print(f.result())