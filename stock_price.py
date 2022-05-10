import requests

import alphavantage.alphavantage as alpha
import polygon.polygon as poly
import pandas as pd
import pyarrow.feather as feather
import datetime
import winsound

alpha.alphavantage_system_set_up(setup_lc=[0,0,1])
poly.polygon_system_setup()

#alphavantage market data and technical indicators
def single_stock_trading_alpha(ticker):
    input_dict = {}
    input_dict["symbol"] = ticker
    input_dict["outputsize"]="full"
    df = pd.DataFrame()
    for function in alpha.field_dict_alpha.keys():
        input_dict["function"] = function
        df_temp = alpha.get_df_market(input_dict)
        df = pd.concat([df, df_temp], axis=1).dropna()
    return df
"""
def test_function(ticker,function):
    input_dict = {}
    input_dict["symbol"] = ticker
    input_dict["function"] = function
    df_temp = get_df(input_dict)
    return df_temp
"""

def single_stock_trading_poly(ticker,t0="2000-01-01",tn=datetime.datetime.today().strftime("%Y/%m/%d")):
    input_dict = {}
    input_dict["ticker"] = ticker
    input_dict['function'] = "stock_hist_data"
    #input_dict['t0']=df_0.index[0].strftime("%Y/%m/%d")
    #input_dict['tn']=df_0.index[-1].strftime("%Y/%m/%d")
    input_dict['t0']=t0
    input_dict['tn']=tn
    df = poly.get_df_poly(input_dict)
    df["index"] = 0
    df["index"] = df["t"].apply(lambda x: poly.unix_time_to_datetime(x))
    df = df.set_index(df["index"]).drop("index", axis=1)
    return df

def cast_datatype_df(df):
    int_lc = ["Aroon Up","Aroon Down","AROONOSC","OBV","volume"]
    cols = df.columns
    for col in cols:
        df[col]=df[col].astype(float)
    for col in int_lc:
        df[col] = df[col].astype(int)
    return df

def master_df_single_stock(ticker):
    print(ticker)
    df = single_stock_trading_alpha(ticker)
    df = df.rename(columns={"1. open":"open", "2. high":"high", "3. low":"low", "4. close":"close", "5. volume":"volume"})
    df = cast_datatype_df(df)
    df["target1d"] = (df["close"].shift(1) - df["close"]) / df["close"]
    df["target1w"] = (df["close"].shift(5) - df["close"]) / df["close"]
    df["target1m"] = (df["close"].shift(21) - df["close"]) / df["close"]
    #df.drop(columns=["1. open", "2. high", "3. low", "4. close", "5. volume"], inplace=True)
    df.dropna(inplace=True)
    #df = pd.merge(df_0, df_1, left_index=True, right_index=True)
    return df

def master_all_stock_df():
    file_path_in = r"C:\Users\Peter Yan\Desktop\repo\peterzergquant\Data\all_liquid_ticker"
    file_path_out = r"C:\Users\Peter Yan\Desktop\repo\peterzergquant\Data\us_equity"
    lc_df = feather.read_feather(file_path_in)
    ticker_lc = lc_df["0"].to_list()
    ticker_lc = [s.replace('.', '-') for s in ticker_lc] #convert polygon ticker format to alphavanatage format
    ticker_lc.sort()
    #df_master = pd.DataFrame()
    count = 2095
    for i in range(count,len(ticker_lc)):
        try:
            temp_df = master_df_single_stock(ticker_lc[i])
            temp_df.reset_index(inplace=True, drop=False)
            temp_df = temp_df.rename(columns={"index": "date"})
            #if df_master.shape==(0,1) or df_master.empty==True:
                #pass
            #else:
                #temp_df = temp_df.reindex(df_master.columns, axis=1)
            #df_master = pd.concat([df_master, temp_df])
            file_path_out_file = file_path_out + "/" +ticker_lc[i]
            feather.write_feather(temp_df, file_path_out_file)#df_master
            count+=1
            print("count=",count)
            #print(df_master.shape)
        except Exception as e:
            winsound.PlaySound("*", winsound.SND_ALIAS)
            raise e
    return None #df_master

def save_option_expr_as_feather(df,ticker):
    path_out = "Data/us_equity/"
    file_path = path_out+ticker
    feather.write_feather(df, file_path)

def read_df_from_feather_single_eqt(ticker):
    path_out = "Data/us_equity"
    file_path = path_out+ticker
    return feather.read_feather(file_path)



#start machine learning
def read_test():
    file_name = "A"
    path = "Data/us_equity/"
    return feather.read_feather(path+file_name)

def test():
    #get all tickers for the project
    file_path_in = r"C:\Users\Peter Yan\Desktop\repo\peterzergquant\Data\all_liquid_ticker"
    lc_df = feather.read_feather(file_path_in)
    ticker_lc = lc_df["0"].to_list()
    ticker_lc.sort()
    dict = {}
    for ticker in ticker_lc:
        url = poly.url_ticker_info(ticker)
        r = requests.get(url)
        j = r.json()
        if "sic_code" in j["results"]:
            dict[ticker] = j["results"]["sic_code"]
        else:
            print(ticker)
    return dict



