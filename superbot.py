import multiprocessing as mp
import threading
import sortedcontainers as sc
import collections
import time
import traceback
import websocket
import json

from datetime import datetime
import calendar

import pybitflyer2
import numpy as np

from logic_plot import Logic_plot as Logic
def main():
    spbot = Superbot()
    spbot.run()
    return

# superbot自体はメインスレッド
class Superbot:
    def __init__(self):
        self.logic = Logic()
        self.container = Container(channel = {"board" : True, "ticker" : True, "executions" : True})
        self.container_thrd = threading.Thread(target=self.container.run, args=(), daemon=True)
        self.logic_thrd = threading.Thread(target=self.logic.run, args=(self.container,), daemon=True)
        
        self.bf = ws_bitflyer()
        self.bf.connect_to_container(self.container)
        self.ws_proc = mp.Process(target=self.bf.run, args=(), kwargs={}, daemon=True)

        self.log = Log()
        self.log_proc = mp.Process(target=self.log.run, args=(), kwargs={}, daemon=True)
        #self.monitor_thrd = threading.Thread(target=, args=(), daemon=True)
        return

    def run(self):
        self.ws_proc.start()
        self.container_thrd.start()
        self.log_proc.start()
        #self.logic_thrd.start()
        self.logic.run(self.container)

        while True:
            time.sleep(1.0)        
        return

class Log:
    def __init__(self):
        return

    def run(self):
        return


class ws_bitflyer:
    def __init__(self, redundancy = 2):
        self.redundancy = redundancy
        if self.redundancy < 1:
            print("# negative redundancy ", redundancy, "is not acceptable.")
            quit()
            
        self.url = "wss://ws.lightstream.bitflyer.com/json-rpc"
        
        self.ws_thrds = []
        self.ws_status = {}
        for i in range(self.redundancy):
            self.ws_thrds.append(threading.Thread(target=self.connect_json_rpc, args=(), daemon=True))
            print("# generating thread : ", self.ws_thrds[-1].getName())
            self.ws_status[self.ws_thrds[-1].getName()] = False
            self.using_thread_name = self.ws_thrds[0].getName()

        self.API = pybitflyer2.API(timeout = 5)            
        self.board_api_thrd = (threading.Thread(target=self.get_board, args=(), daemon=True))
        self.channels = []

        self.lock = threading.Lock()
        return
    
    def connect_to_container(self, container):
        self.channels = []
        if container.channel["board"]:
            self.channels.append("lightning_board_FX_BTC_JPY")
            #self.channels.append("lightning_board_snapshot_FX_BTC_JPY")
            self.board_writer = container.board_writer
        if container.channel["ticker"]:
            self.channels.append("lightning_ticker_FX_BTC_JPY")
            self.ticker_writer = container.ticker_writer
        if container.channel["executions"]:
            self.channels.append("lightning_executions_FX_BTC_JPY")
            self.executions_writer = container.executions_writer
        return
    
    def run(self):
        if "lightning_board_FX_BTC_JPY" in self.channels:
            self.board_api_thrd.start()
        for i in range(self.redundancy):
            self.ws_thrds[i].start()
            
        while True:
            time.sleep(0.1)
            if not self.ws_status[self.using_thread_name]:
                for i in range(self.redundancy):
                    if self.using_thread_name != self.ws_thrds[i]:
                        self.using_thread_name = self.ws_thrds[i].getName()
                        
        return

    def handle_message_json_rpc(self, message):
        #print(message["params"]["channel"])
        if threading.current_thread().getName() != self.using_thread_name:
            return
        try:
            if message["params"]["channel"] == "lightning_board_FX_BTC_JPY":
                self.lock.acquire()
                self.board_writer.send(message["params"]["message"])
                self.lock.release()
            elif message["params"]["channel"] == "lightning_board_snapshot_FX_BTC_JPY":
                self.board_writer.send(message["params"]["message"])
            elif message["params"]["channel"] == "lightning_ticker_FX_BTC_JPY":
                self.ticker_writer.send(message["params"]["message"])
            elif message["params"]["channel"] == "lightning_executions_FX_BTC_JPY":
                self.executions_writer.send(message["params"]["message"])
        except:
            print(traceback.format_exc(), flush=True)
        return

    def get_board(self, timespan = 5):
        last_get_time = 0
        while True:
            if time.time() > last_get_time + timespan:
                try:
                    res = self.API.board(product_code = "FX_BTC_JPY")
                    if "mid_price" in res.keys():
                        self.lock.acquire()
                        self.board_writer.send(res)
                        self.lock.release()
                        last_get_time = time.time()
                except:
                    print(traceback.format_exc())
            else:
                time.sleep(0.01)
        return
    
    def connect_json_rpc(self):
        def on_message(ws, message):
            try:
                message = json.loads(message)
                self.handle_message_json_rpc(message)
            except:
                print(traceback.format_exc(), flush=True)
            return
        
        def on_error(ws, error):
            print(traceback.format_exc())
            print(error, flush=True)
            return
        
        def on_close(ws):
            print("# websocket closed", flush=True)
            self.ws_status[threading.current_thread().getName()] = False
            try:
                ws.close()
            except:
                print(traceback.format_exc())
            finally:
                time.sleep(3)
                ws = make_ws()
                ws.run_forever()
            return
        
        def on_open(ws):
            for ch in self.channels:
                ws.send(json.dumps({"method" : "subscribe", "params" : {"channel" : ch}}))
            print("# bitflyer connected 1", flush=True)
            self.ws_status[threading.current_thread().getName()] = True
            return
        
        def make_ws(timeout = 5):
            while True:
                try:
                    ws = websocket.WebSocketApp(self.url,
                                        on_message = on_message,
                                        on_error = on_error,
                                        on_close = on_close,
                                        on_open = on_open)
                    print("# generated websocket 1", flush=True)
                    break
                except Exception as e:
                    print(traceback.format_exc())
            ws.ping_timeout = timeout

            return ws

        websocket.enableTrace(True)
        ws = make_ws()
        ws.run_forever()
        return
    
    
class Container:
    def __init__(self, channel = {"board" : True, "ticker" : True, "executions" : True}):
        self.channel = channel
        board_data_size = 10
        self.board_ask = np.array([[],[]])
        self.board_bid = np.array([[],[]])
        self.tmp_board_ask = sc.SortedDict()
        self.tmp_board_bid = sc.SortedDict()
        self.board_time = collections.deque([], board_data_size)
        self.board_ask_history = collections.deque([], board_data_size)
        self.board_bid_history = collections.deque([], board_data_size)
        self.board_reader, self.board_writer = mp.Pipe(duplex=False)
            
        data_size = 10000
        self.ticker_time = collections.deque([], data_size)
        self.ticker_bid = collections.deque([], data_size)
        self.ticker_ask = collections.deque([], data_size)
        self.execution_time = collections.deque([], data_size)
        self.execution_side = collections.deque([], data_size)
        self.execution_price = collections.deque([], data_size)
        self.execution_size = collections.deque([], data_size)


        self.ticker_reader, self.ticker_writer = mp.Pipe(duplex=False)
        self.executions_reader, self.executions_writer = mp.Pipe(duplex=False)
        
        return

    def run(self):
        if self.channel["board"]:
            self.board_thrd = threading.Thread(target=self.update_board, args=(), daemon=True)
            self.board_thrd.start()
        if self.channel["ticker"]:            
            self.ticker_thrd = threading.Thread(target=self.update_ticker, args=(), daemon=True)
            self.ticker_thrd.start()            
        if self.channel["executions"]:
            self.executions_thrd = threading.Thread(target=self.update_executions, args=(), daemon=True)
            self.executions_thrd.start()
        return

    def update_board(self):
        try:
            while True:
                if self.board_reader.poll():
                    message = self.board_reader.recv()
                    self.tmp_board_ask.update([(d.get('price', 0), d.get('size', 0)) for d in message["asks"]])
                    self.tmp_board_bid.update([(d.get('price', 0), d.get('size', 0)) for d in message["bids"]])
                    mid_price = message["mid_price"]
                    for d in self.tmp_board_ask:
                        if d < mid_price:
                            self.tmp_board_ask[d] = 0
                        else:
                            break
                    for d in reversed(self.tmp_board_bid):
                        if d > mid_price:
                            self.tmp_board_bid[d] = 0
                        else:
                            break
                    self.board_ask = np.array([self.tmp_board_ask.keys(), self.tmp_board_ask.values()])
                    self.board_bid = np.array([self.tmp_board_bid.keys(), self.tmp_board_bid.values()])

                    self.board_time.append(time.time())                    
                    self.board_ask_history.append(self.board_ask)
                    self.board_bid_history.append(self.board_bid)

                time.sleep(0.001)
        except:
            print(traceback.format_exc())
            self.update_board()
            
        return
    
    def update_ticker(self):
        try:
            while True:
                if self.ticker_reader.poll():
                    message = self.ticker_reader.recv()
                    utc = datetime.strptime(message["timestamp"].split(".")[0], '%Y-%m-%dT%H:%M:%S')
                    utc = calendar.timegm(utc.timetuple()) + float("0." + message["timestamp"].strip("Z").split(".")[-1])
                    bid = message["best_bid"]
                    ask = message["best_ask"]
                    self.ticker_time.append(utc)
                    self.ticker_bid.append(bid)
                    self.ticker_ask.append(ask)
                time.sleep(0.001)
        except:
            print(traceback.format_exc())
            self.update_ticker()
        return
    
    def update_executions(self):
        try:
            while True:
                if self.executions_reader.poll():
                    message = self.executions_reader.recv()
                    i = message[0]
                    utc = datetime.strptime(i["exec_date"].split(".")[0], '%Y-%m-%dT%H:%M:%S')
                    utc = calendar.timegm(utc.timetuple()) + float("0." + i["exec_date"].strip("Z").split(".")[-1])                    
                    for i in message:
                        price = i["price"]
                        size = i["size"]
                        side = i["side"]
                        self.execution_time.append(utc)
                        self.execution_price.append(price)
                        self.execution_size.append(size)
                        if side == "BUY":
                            self.execution_side.append(1)
                        elif side == "SELL":
                            self.execution_side.append(-1)
                        
                time.sleep(0.001)
        except:
            print(traceback.format_exc())
            self.update_executions()

        return    
    
if __name__ == "__main__":
    main()
