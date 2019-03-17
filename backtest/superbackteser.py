from collections import namedtuple
import numpy as np
import pandas as pd
import sys
import pickle
import matplotlib.pyplot as plt
import time

def main():
    fname = sys.argv[1]
    with open(fname, "rb") as f:
        df = pickle.load(f)
        
    tohlcv = df.values
    bt = Backtester()
    pos, dep = bt.run(tohlcv)

    plt.figure()
    plt.subplot(211)
    plt.plot(tohlcv[:,0], dep + pos * tohlcv[:,4])
    plt.subplot(212)
    #plt.plot(tohlcv[:,0], tohlcv[:,4])
    plt.plot(tohlcv[:,0], pos, "k", lw=0.5)
    plt.twinx()
    #plt.plot(tohlcv[:,0], dep, "g", lw=0.5)
    plt.show()
    return

Order = namedtuple('Order', ('side', 'order_type', 'price', 'size', 'expire_time', 'put_time'))
class Superbacktester:
    def __init__(self, iprint = 0):
        self.iprint = iprint
        self.order_list = []
        self.deposit = 0.0
        self.position = 0.0
        self.average_price = np.nan
        self.delay = 0.0
        self.time = 0.0
        self.resolution = 1.0
        self.LIMIT = 0
        self.MARKET = 1
        return

    def run(self, tohlcv):
        """
        tohlcv : numpy.array ( len, 6)
        t : the opening time of the candle
        """
        dsize = len(tohlcv)

        self.resolution = tohlcv[1, 0] - tohlcv[0, 0]
        self.pos = np.zeros(dsize)
        self.dep = np.zeros(dsize)
        for i in range(dsize):
            self.time = tohlcv[i]

            """
            strategy
            compute, entry, exit, etc
            """
            self.trade(tohlcv[:i+1])
            self.check_execution(tohlcv[:i+1])
            
            """
            log
            """            
            self.pos[i] = self.position
            self.dep[i] = self.deposit

        self.callback()
        return self.pos, self.dep

    def trade(self, tohlcv):
        return

    def callback(self):
        return
    
    def put_order(self, side, order_type, price, size, duration = 43200):
        self.order_list.append(Order(price = price,
                                     side = side,
                                     size = size,
                                     order_type = order_type, # 0 : limit, 1 : market
                                     expire_time = self.time + self.resolution + duration,
                                     put_time = self.time + self.resolution))
        return
    
    def check_execution(self, tohlcv):
        dellist = []
        for j, order in enumerate(self.order_list):
            if order.order_type == 1:
                if order.side == 1:
                    self.entry(order.side, tohlcv[-1, 2], order.size)
                    dellist.append(j)
                elif order.side == -1:
                    self.entry(order.side, tohlcv[-1, 3], order.size)
                    dellist.append(j)  

            else: # order.order_type == 0
                flag1 = tohlcv[-1, 0] + self.resolution > (order.put_time + self.delay)
                flag2 = tohlcv[-1, 0] + self.resolution <= order.expire_time
                flag_full_execution = False
                flag_partial_execution = False
                if order.side == 1: 
                    flag_full_execution = tohlcv[i, 3] < order.price and tohlcv[i, 5] > order.size
                    flag_partial_execution = tohlcv[i, 3] < order.price and tohlcv[i, 5] <= order.size
                else : #side == -1
                    flag_full_execution = tohlcv[i, 2] > order.price and tohlcv[i, 5] > order.size
                    flag_partial_execution = tohlcv[i, 3] < order.price and tohlcv[i, 5] <= order.size
                
                if flag1 and flag2:
                    if flag_full_execution:
                        self.entry(order.side, order.price, order.size)
                        dellist.append(j)
                    elif flag_partial_execution:
                        self.entry(order.side, order.price, tohlcv[i, 5].size)
                        self.order_list[j].size = order.size - tohlcv[i, 5]
                    
                elif not flag2:
                    dellist.append(j)
                
        self.delete_order(dellist)
        return    

    def delete_order(self, dellist):
        for idx in dellist[::-1]:
            self.order_list.pop(idx)
        return    

    def entry(self, direction, price, size):
        # direction == 1 -> long
        # direction == -1 -> short
        #print("e1", self.deposit)
        old_pos = self.position
        self.deposit -= direction * price * size
        self.position += direction * size
        
        if np.abs(self.position) < 0.001:
            self.position = 0
            self.average_price = np.nan
        elif np.abs(self.position) > 0.01 and not np.isfinite(self.average_price):
            #ポジションがあるのに、ポジション価格にnanを検知したとき
            self.average_price = price
        elif np.abs(self.position) > 0 and old_pos * self.position <= 0:
            #ポジションの方向が変わったとき
            self.average_price = price
        elif np.abs(self.position) > np.abs(old_pos) > 0 and old_pos * self.position > 0:
            # L -> L or S -> S
            a = self.average_price * np.abs(old_pos)
            b = price * np.abs(size)
            self.average_price = (a + b) / np.abs(self.position)

        return

class Backtester(Superbacktester):
    def __init__(self, iprint = 0):
        super().__init__(iprint)
        """
        parameter
        """
        self.max_pos = 0
        self.a = []
        self.start_time = time.time()
        self.total_order = 0
        return

    def trade(self, tohlcv):
        if np.abs(self.position) < 0.0001:
            self.put_order(+1, self.MARKET, price = 0, size = 1, duration = 43200)
            self.total_order += 1
        elif self.position > 0 and self.average_price - 100 > tohlcv[-1,2]:
            self.put_order(+1, self.MARKET, price = 0, size = self.position, duration = 43200)
            self.total_order += 1
        elif self.position > 0 and self.average_price + 10 < tohlcv[-1,3]:
            self.put_order(-1, self.MARKET, price = 0, size = self.position, duration = 43200)
            self.total_order += 1
        return

    def callback(self):
        print("# elapsed time : ", time.time() - self.start_time)
        print(self.total_order)
        return
        
    
if __name__ == "__main__":
    main()




