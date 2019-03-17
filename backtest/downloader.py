import pybitflyer2 as PBF
import pandas as pd
import datetime
import calendar
import time
import pickle
import traceback
pbf = PBF.API()


def main():
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days = 1)
    #tday = today
    tday = yesterday
    start_time = datetime.datetime(tday.year, tday.month, tday.day, 0, 0)
    end_time = datetime.datetime(tday.year, tday.month, tday.day, 23, 59)
    df = None
    last_id = None
    while True:
        time.sleep(0.2)
        try:
            df0 = save_executions(last_id, product_code = "FX_BTC_JPY")
        except:
            print(traceback.format_exc())
            continue
        last_id = df0.iloc[-1]["id"]        
        time1 = datetime.datetime.fromtimestamp(df0["unixtime"].iloc[0]) # end
        time0 = datetime.datetime.fromtimestamp(df0["unixtime"].iloc[-1]) # start
        print(time0, time1, start_time, end_time)
        if df is not None:
            print(len(df))
        else:
            print(0)
        
        if time0 > end_time:
            continue
        elif time1 < start_time:
            break
        
        if df is None:
            df = df0
        else:
            df = pd.concat([df, df0], ignore_index=True)


              
              
        #quit()

    start = datetime.datetime.fromtimestamp(df0["unixtime"].iloc[-1])
    end = datetime.datetime.fromtimestamp(df0["unixtime"].iloc[0])      
    fname = str(end).split()[0] + ".pkl"
    with open(fname, "wb") as f:
        pickle.dump(df, f)
        
    return

def save_executions(last_id=None, product_code = "FX_BTC_JPY"):
    #show_board(pbf.board(product_code = "FX_BTC_JPY"))
    if last_id is None:
        data = pbf.executions(product_code = product_code,
                              count = 500)
    else:
        data = pbf.executions(product_code = product_code,
                              count = 500,
                              before = last_id)
        
    df1 = pd.DataFrame(data)
    df1 = df1[["exec_date", "id", "price", "side", "size"]]
    df2 = df1[["exec_date"]].applymap(convert)
    df0 = pd.concat([df2, df1], axis=1)

    #print(df1.columns)
    df0.columns = ["unixtime"] + list(df1.columns)
    return df0
    
def convert(strtime):
    if len(strtime.split(".")) == 1:
        strtime += ".0"
    utc = datetime.datetime.strptime(strtime.split(".")[0], '%Y-%m-%dT%H:%M:%S')
    return calendar.timegm(utc.timetuple()) + float("0." + strtime.strip("Z").split(".")[-1])

        




if __name__ =="__main__":
    main()

