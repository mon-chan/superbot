import multiprocessing as mp
import threading
import concurrent.futures
import sortedcontainers as sc
import collections
import time
import traceback
import websocket
import json

def main():
    spbot = Superbot()
    spbot.run()
    print("aaa")
    return

# superbot自体はメインスレッド
class Superbot:
    def __init__(self):
        self.logic = Logic()
        self.container = Container()
        self.container_thrd = threading.Thread(target=self.container.run, args=(), daemon=True)
        self.logic_thrd = threading.Thread(target=self.logic.run, args=(self.container,), daemon=True)
        
        self.bf = ws_bitflyer()
        self.bf.connect_to_container(self.container)
        self.ws_proc = mp.Process(target=self.bf.run, args=(), kwargs={}, daemon=True)

        #self.log_proc = mp.Process(target=関数, args=(), kwargs={}, daemon=True)
        #self.monitor_thrd = threading.Thread(target=, args=(), daemon=True)
        return

    def run(self):
        self.ws_proc.start()
        self.container_thrd.start()
        self.logic_thrd.start()

        while True:
            time.sleep(1.0)        
        return


class Logic:
    def __init__(self, max_workers = 6):
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers)
        return
    def run(self, container):
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
            
        self.channels = []
        return

    def connect_to_container(self, container):
        self.channels = []
        if container.channel["board"]:
            self.channels.append("lightning_executions_FX_BTC_JPY")
            self.board_writer = container.board_writer
        if container.channel["ticker"]:
            self.channels.append("lightning_ticker_FX_BTC_JPY")
            self.ticker_writer = container.ticker_writer
        if container.channel["executions"]:
            self.channels.append("lightning_board_FX_BTC_JPY")
            self.executions_writer = container.executions_writer
        return
    
    def run(self):
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
        if threading.current_thread().getName() != self.using_thread_name:
            return
        try:
            if message["params"]["channel"] == "lightning_board_FX_BTC_JPY":
                self.board_writer.send(message["params"])
            elif message["params"]["channel"] == "lightning_ticker_FX_BTC_JPY":
                self.ticker_writer.send(message["params"])
            elif message["params"]["channel"] == "lightning_executions_FX_BTC_JPY":
                self.executions_writer.send(message["params"])
        except:
            print(traceback.format_exc(), flush=True)
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
            print("# bitflyer connecting 1", flush=True)
            for ch in self.channels:
                ws.send(json.dumps({"method" : "subscribe", "params" : {"channel" : ch}}))
            print("# bitflyer connected 1", flush=True)
            self.ws_status[threading.current_thread().getName()] = True
            return
        
        def make_ws(timeout = 5):
            while True:
                try:
                    print("# generating websocket 1", flush=True)
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
        self.board = sc.SortedDict()
        self.board_ask = sc.SortedDict()
        self.board_bid = sc.SortedDict()
        self.board_time = collections.deque([], board_data_size)
        self.board_history = collections.deque([], board_data_size)
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
            self.executions_thrd = threading.Thread(target=self.update_executions, args=(), daemon=True)
        if self.channel["executions"]:
            self.ticker_thrd.start()
            self.executions_thrd.start()
        return

    def update_board(self):
        while True:
            if self.board_reader.poll():
                print(self.board_reader.recv()["channel"])
                time.sleep(0.01)        
        return
    
    def update_ticker(self):
        while True:
            if self.ticker_reader.poll():
                print(self.ticker_reader.recv()["channel"])
                time.sleep(0.01)        
        return
    
    def update_executions(self):
        while True:
            if self.board_reader.poll():
                print(self.executions_reader.recv()["channel"])
                time.sleep(0.01)
        return    
    
if __name__ == "__main__":
    main()
