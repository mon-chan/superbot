import concurrent.futures
import time

class Logic:
    def __init__(self, max_workers = 6):
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers)
        return
    
    def run(self, container, min_len = 3):
        while not (len(container.ticker_time) > min_len and len(container.execution_time) > min_len):
            print(len(container.ticker_time))
            time.sleep(0.1)

        """
        while True:
            if container.execution_time[-1] > last_execution_time:
                print(container.execution_time[-1], container.ticker_bid[-1], container.ticker_ask[-1], container.execution_price[-1])
                last_execution_time = container.execution_time[-1]
                print(datetime.fromtimestamp(container.execution_time[-1]))
                time.sleep(0.01)
        """            
        return
