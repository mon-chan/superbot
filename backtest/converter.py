import pandas as pd
import sys
import pickle
import datetime
import numpy as np
def main():

    fname = sys.argv[1]
    with open(fname, "rb") as f:
        df = pickle.load(f)    
        
    df["datetime"] = df["unixtime"].apply(lambda x: datetime.datetime.fromtimestamp(x))
    df = df.set_index("datetime")

    dum = pd.get_dummies(df["side"])
    dside = dum["BUY"].astype(np.int64) - dum["SELL"].astype(np.int64)
    sgnsize = dum["BUY"] * df["size"] - dum["SELL"] * df["size"]
    df["sgnsize"] = sgnsize
    
    resolution = "1s"
    ohlc = df["price"].resample(resolution).ohlc().ffill()
    vol = df["sgnsize"].resample(resolution).sum()
    ndf = pd.concat([ohlc, vol], axis=1)
    ndf["unixtime"] = pd.to_datetime(ndf.index).tz_localize('Asia/Tokyo').astype('int64')/10**9
    #print(ndf)
    ndf2 = ndf[["unixtime", "open", "high", "low", "close", "sgnsize"]]
    
    fname = fname.split(".")[0] + "_" + resolution + ".pkl"    
    with open(fname, "wb") as f:
        pickle.dump(ndf2, f)
    
if __name__ == "__main__":
    main()
