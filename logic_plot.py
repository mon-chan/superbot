from logic import Logic
import numpy as np
import time
import pyqtgraph as pg
from pyqtgraph.Qt import QtGui, QtCore

class Logic_plot(Logic):
    def __init__(self, max_workers = 6):
        super().__init__(max_workers)

    def run(self, container, min_len = 3):
        super().run(container, min_len = 3)
        class Plotter(pg.GraphicsWindow):
            def __init__(self, interval_ms = 250):
                super().__init__()
                self.interval_ms = interval_ms
                self.resize(800,400)
                self.clist = ("w", "b", "g", "r", "c", "m", "y")
                self.plt1 = self.addPlot()
                self.line1_ask = self.plt1.plot(pen=pg.mkPen(color="r", width=1.0), antialias=True)
                self.line1_bid = self.plt1.plot(pen=pg.mkPen(color="g", width=1.0), antialias=True)
                self.nextRow()
                self.plt2 = self.addPlot()
                self.line2_ask = self.plt2.plot(pen=pg.mkPen(color="r", width=1.0), antialias=True)
                self.line2_bid = self.plt2.plot(pen=pg.mkPen(color="g", width=1.0), antialias=True)
                self.nextRow()
                self.plt3 = self.addPlot()
                self.line3 = self.plt3.plot(pen=pg.mkPen(color="w", width=1.0), antialias=True)
                self.timer = QtCore.QTimer()
                self.timer.timeout.connect(self.update_plot)
                self.timer.start(self.interval_ms)
                return
            
            def update_plot(self):
                asks = np.copy(container.board_ask)
                bids = np.copy(container.board_bid)
                mid_price = 0.5*(container.ticker_bid[-1] + container.ticker_ask[-1])
                y1 = asks[1,:]
                y2 = bids[1,::-1]
                
                self.line1_ask.setData(list(asks[0,:]), list(y1))
                self.line1_bid.setData(list(bids[0,::-1]), list(y2))
                prange = 3000
                a = asks[0,:] > container.ticker_ask[-1]
                b = asks[0,:] < container.ticker_ask[-1] + prange
                c = np.logical_and(a, b)
                ymax = np.max(y1[np.where(c)])                
                self.plt1.setXRange(mid_price - prange, mid_price + prange)
                self.plt1.setYRange(0, ymax)
                
                self.line2_ask.setData(list(asks[0,:]), list(y1))
                self.line2_bid.setData(list(bids[0,::-1]), list(y2))
                prange = 500
                a = asks[0,:] > container.ticker_ask[-1]
                b = asks[0,:] < container.ticker_ask[-1] + prange
                c = np.logical_and(a, b)
                ymax = np.max(y1[np.where(c)])
                self.plt2.setXRange(mid_price - prange, mid_price + prange)
                self.plt2.setYRange(0, ymax)                
                
                dsize = 5000
                size = np.array(container.execution_size)[-dsize:]
                side = np.array(container.execution_side)[-dsize:]
                price = np.array(container.execution_price)[-dsize:]
                etime = np.array(container.execution_time)[-dsize:]
                #x = np.arange(dsize)
                x = etime - time.time()
                y = np.cumsum(size * side)
                print(len(x), len(y))
                self.line3.setData(x, y)
                self.plt3.setXRange(np.min(x), np.max(x))
                self.plt3.setYRange(np.min(y), np.max(y))
                
        app = QtGui.QApplication([])
        plotter = Plotter()
        app.exec_()
        quit()
        
                
                
