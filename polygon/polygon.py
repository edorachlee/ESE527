##imports
#external library
import requests
import pandas as pd
import re
from datetime import datetime, timedelta
import pyarrow.feather as feather
#in repo packages
import api_keys

#add parent path
import sys
sys.path.append("/Users/zerg/Desktop/Repos/PeterZergQuant")

#set up, hard code
base_url_poly = "https://api.polygon.io/"
api_url_poly = "&apiKey="+api_keys.poly_dict["api_key"]
field_dict_poly = {}
req_dict_poly = {}
field_value_dict_poly = {}
default_value_dict_poly = {}
function_dict = {}
dict_LC_poly = [field_dict_poly, req_dict_poly, field_value_dict_poly, default_value_dict_poly, function_dict]

#r to def function
def from_request_to_df(r):
    j = r.json()
    for key in j:
        if key!='Meta Data':
            d=j[key]
    df = pd.DataFrame.from_dict(d).transpose()
    df.index = pd.to_datetime(df.index)
    return df

#helper function
def int_check(input,cap=50000):
    """
    :param input: input to check
    :param cap: max int value allowed
    :return: None if not valid, correct input if pass test (fully or fixable)
    """
    if type(input)!=int:
        if type(input)==float:
            if input.is_integer()==True:
                input = int(input)
            else:
                print("ERROR: arg timeintervalscale, not an int but a float")
                return None
        else:
            print("ERROR: arg timeintervalscale, not an int")
            return None
    elif input<1:
        print("ERROR: arg timeintervalscale can not be less than 1")
        return None
    elif input>cap:
        print("Attension: input>max value cap, auto scale input to cap")
        input = cap
    return input

def url_gen_wrapper_poly(func, args_LC):
    """
    :param func: function
    :param args_LC: list of arg inputs
    :return: string of url
    """
    return func(*args_LC)

def unix_time_to_datetime(unix_int):
    temp_dt = datetime(1970,1,1)+timedelta(milliseconds=unix_int) #UTC
    dt = temp_dt.replace(hour=0, minute=0)
    return dt

def single_stock_trading_poly(ticker,t0="2000-01-01",tn=datetime.today().strftime("%Y/%m/%d")):
    input_dict = {}
    input_dict["ticker"] = ticker
    input_dict['function'] = "stock_hist_data"
    #input_dict['t0']=df_0.index[0].strftime("%Y/%m/%d")
    #input_dict['tn']=df_0.index[-1].strftime("%Y/%m/%d")
    input_dict['t0']=t0
    input_dict['tn']=tn
    df = get_df_poly(input_dict)
    return df

def filter_ticker(ticker_lc):
    output_lc = []
    bench_mark_t_lc = SPY_df_raw()["t"].tolist()
    for ticker in ticker_lc:
        include = True
        df = single_stock_trading_poly(ticker)
        stock_t_lc = df["t"].tolist()
        #condition checks
        if len(stock_t_lc) < 1265:
            include = False
        elif check_liquidity(stock_t_lc,bench_mark_t_lc)==False:
            include = False
        #determine
        if include==True:
            output_lc.append(ticker)
    return output_lc

def SPY_df_raw():
    url = "https://api.polygon.io/v2/aggs/ticker/SPY/range/1/day/2000-01-01/2022-05-07?adjusted=true&sort=asc&limit=50000&apiKey=GNthmWT9qYGm57QwnIJ_orim_uN5mbc0"
    r = requests.get(url)
    j = r.json()
    df = pd.DataFrame(j["results"])
    return df

def check_liquidity(stock_t_lc,bench_mark_t_lc):
    for i in range(len(stock_t_lc)-1):
        next_day = stock_t_lc[i]+86400000
        if (next_day in stock_t_lc)==False:
            if (next_day in bench_mark_t_lc):
                return False
    return True

#### Url functions for polygon.io
#system setup():

def polygon_system_setup():
    global dict_LC_poly
    global field_dict_poly
    global req_dict_poly
    global field_value_dict_poly
    global default_value_dict_poly
    default_value_dict_poly["function"]="stock_hist_data"
    #stock_hist_data
    function="stock_hist_data"
    function_dict[function]=url_stock_market_data
    field_LC = ["ticker","t0","tn",
                "timeintervalscale","timeinterval","price_adj","sortAD","limit"]
    field_dict_poly[function] = field_LC
    req_LC = [1,1,1,0,0, 0,0,0]
    for i in range(len(field_LC)):
        req_dict_poly[(function, field_LC[i])] = req_LC[i]
    default_value_dict_poly["timeintervalscale"] = "day"
    default_value_dict_poly["timeinterval"] = 1
    default_value_dict_poly["price_adj"] = True
    default_value_dict_poly["sortAD"] = True
    field_value_dict_poly["timeinterval"] = ["miniute","hour","day","week","month","quarter","year"]
    #stock_hist_data_singelday
    function = "stock_hist_data_singelday"
    function_dict[function] = url_stock_market_singleday
    field_LC = ["ticker", "t0",
                "price_adj", ]
    field_dict_poly[function] = field_LC
    req_LC = [1, 1, 0]
    for i in range(len(field_LC)):
        req_dict_poly[(function, field_LC[i])] = req_LC[i]
    #dict record
    function="stock_hist_data_orderflow"
    function_dict[function] = url_trade_orderflow
    field_LC = ["ticker",
                "timestamp","sortAD","limit","sortTimestamp"]
    field_dict_poly[function] = field_LC
    req_LC = [1, 0,0,0,0]
    for i in range(len(field_LC)):
        req_dict_poly[(function, field_LC[i])] = req_LC[i]
    default_value_dict_poly["timestamp"]=True
    default_value_dict_poly["sortTimestamp"] = True
    # financials
    function = "stock_financial"
    function_dict[function] = url_financials
    field_LC = ["ticker",
                "timeframe_FS", "include_sourcesTF", "sortAD", "limit", "sortRF"]
    field_dict_poly[function] = field_LC
    req_LC = [1, 0, 0, 0, 0, 0]
    for i in range(len(field_LC)):
        req_dict_poly[(function, field_LC[i])] = req_LC[i]
    default_value_dict_poly["timeframe_FS"] = "annual"
    default_value_dict_poly["include_sourcesTF"] = True
    default_value_dict_poly["sortRF"] = True
    field_value_dict_poly["timeframe_FS"]=["annual","quarterly",None]


#market data
def url_stock_market_data(ticker,t0,tn,timeintervalscale=1,timeinterval="day",price_adj=True,sortAD=True,limit=5000):
    """
    :param ticker: stock ticker, str, no exchange attach
    :param timeintervalscale: scaling factor for timeinterval, int>0
    :param timeinterval: size of the time window: [miniute,hour,day,week,month,quarter,year]
    :param t0: starting date, YYYY-MM-DD
    :param tn: end date, YYYY-MM-DD
    :param price_adj: Bool, results adjusts for splits. default adjusted=true
    :param sortAD: Bool, sort the results by timestamp, true=ascend=old at top, false=descend=new at top
    :param limit: Limits the number of base aggregates queried. Max 50000, default 5000.
    :return: url in str
    """
    #hard code:
    version= "v2"
    data_path = "aggs"
    #checks
    #t0 & tn
    pattern = '[1|2][0|9][0-9][0-9]-[0|1][0-9]-[0123][0-9]'
    pattern_alt = '[1|2][0|9][0-9][0-9]/[0|1][0-9]/[0123][0-9]'
    #t0
    if re.match(pattern_alt, t0):
        t0 = t0.replace('/', '-', 2)
    if not (re.match(pattern, t0)):
        print("ERROR: t0 not matching format for poly, YYYY/MM/DD")
    #tn
    if re.match(pattern_alt, tn):
        tn = tn.replace('/', '-', 2)
    if not (re.match(pattern, tn)):
        print("ERROR: tn not matching format for poly, YYYY/MM/DD")
    #timeintervalscale
    if int_check(timeintervalscale)!=None:
        timeintervalscale=int_check(timeintervalscale)
    else:
        return None
    #timeinterval
    if timeinterval not in field_value_dict_poly["timeinterval"]:
        print("ERROR: arg timeinterval not valid")
        print("Valid Examples: "+field_value_dict_poly["timeinterval"])
        return None
    #limit
    if int_check(limit) != None:
        limit = int_check(limit)
    else:
        return None
    #not going to check t0 and tn since polygon checks it
    #function fields
    #adj
    if price_adj==True:
        price_adj="true"
    else:
        price_adj="false"
    #sort
    if sortAD==True:
        order="asc"
    else:
        order="desc"

    #url
    req_url = version+"/"+data_path+"/ticker/"+ticker+"/range/"+str(timeintervalscale)+"/"+timeinterval+"/"+t0+"/"+tn
    optional_url = "?"+"adjusted="+price_adj+"&sort="+order+"&limit="+str(limit)
    url = base_url_poly + req_url + optional_url + api_url_poly
    return url
    #January 1, 1970 is the starting time for "n", The Unix Msec timestamp

def url_stock_market_singleday(ticker,t0,price_adj=True):
    """
    :param ticker: stock ticker, str, no exchange
    :param t0: date, YYYY-MM-DD
    :param price_adj: Bool, results adjusts for splits. default adjusted=true
    :return: url in str
    """
    #hard code:
    version= "v1"
    data_path = "open-close"
    #function fields
    #adj
    if price_adj==True:
        price_adj="true"
    else:
        price_adj="false"
    #url
    req_url = version + "/" + data_path + "/" + ticker + "/" + t0
    optional_url = "?" + "adjusted=" + price_adj
    url = base_url_poly + req_url + optional_url + api_url_poly
    return url

def url_trade_orderflow(ticker,timestamp=None,sortAD=True,limit=5000,sortTimestamp=True):
    """
    :param ticker: stock ticker, str, no exchange attach
    :param timestamp: e.g. "2022-04-08", YYYY-MM-DD, if None it would return the whole database
    :param sortAD: true=asc, false=desc
    :param limit: default 5000
    :return: url in str
    """
    #hard code:
    version= "v3"
    data_path = "trades"
    #function fields
    order = "asc" if sortAD == True else "desc"
    timestamp_sort="timestamp" if sortTimestamp == True else None
    #url
    req_url = version+"/"+data_path+"/"+ticker
    optional_url = "?"
    if timestamp!=None:
        optional_url+="timestamp="+timestamp
    optional_url+="&order="+order+"&limit="+str(limit)
    if sortTimestamp!=False:
        optional_url+="&sort=" + timestamp_sort
    url = base_url_poly + req_url + optional_url + api_url_poly
    return url

#TODO
#ref data
def url_all_tradable_tickers():
    "https://api.polygon.io/v3/reference/tickers?type=CS&market=stocks&active=true&sort=ticker&order=asc&limit=1000&apiKey=GNthmWT9qYGm57QwnIJ_orim_uN5mbc0"

def url_financials(ticker,timeframe_FS="annual",include_sourcesTF=True,sortAD=True,limit=100,sortRF=True):
    #hard code:
    version= "vX"
    data_path = "reference"
    req_url = version + "/" + data_path + "/" + "financials?ticker=" + ticker
    #options
    include_sources = "true" if include_sourcesTF==True else "false"
    order="asc" if sortAD==True else "desc"
    sortRF_str = "period_of_report_date" if sortRF==True else "filing_date"
    limit_str = str(limit)
    #url optional
    optional_url = "&timeframe="+timeframe_FS+"&include_sources="+include_sources+"&order="+order+"&limit="+limit_str+"&sort="+sortRF_str
    url = base_url_poly + req_url + optional_url + api_url_poly
    return url

def url_tickers(exchange_MIC,type="CS",market="stocks",active="true",sort="ticker",order="asc",limit="1000"):
    #hard code:
    version= "v3"
    data_path = "reference"
    req_url = version + "/" + data_path + "/" + "tickers?"
    #options
    optional_url = "type="+type+"&market="+market+"&exchange="+exchange_MIC+"&active="+active+"&sort="+sort+"&order="+order+"&limit="+limit
    url = base_url_poly + req_url + optional_url + api_url_poly
    return url

def url_ticker_info(ticker):
    version= "v3"
    data_path = "reference"
    req_url = version + "/" + data_path + "/tickers/" + ticker
    url = base_url_poly + req_url + "?apiKey="+api_keys.poly_dict["api_key"]
    return url

#action
def get_url(input_dict):
    #check if function in dict, else set to default
    if "function" in input_dict:
        function = input_dict["function"]
    else:
        function = default_value_dict_poly["function"]
    #get url
    ref_arg_LC = field_dict_poly[function]
    args_LC = []#lc of args to pass into the url generation functions
    for input_arg in ref_arg_LC:
        if req_dict_poly[(function,input_arg)]==1:
            if input_arg in input_dict:
                args_LC.append(input_dict[input_arg])
            else:
                print("ERROR: Missing function arg: "+input_arg)
                return None
        else:
            if input_arg in input_dict:
                args_LC.append(input_dict[input_arg])
    func = function_dict[function]
    url = url_gen_wrapper_poly(func, args_LC)
    return url

#main
def get_df_poly(input_dict):
    #check if function in dict, else set to default
    if "function" in input_dict:
        function = input_dict["function"]
    else:
        function = default_value_dict_poly["function"]
    #get url
    url = get_url(input_dict)
    next_url = None
    r = requests.get(url)
    j = r.json()
    #check status
    if j['status']!="OK":
        print("status: "+j['status'])
        return None
    if "next_url" in j.keys():
        next_url = j["next_url"]
    #extract
    if function == "stock_hist_data":
        result = j['results']
        df = pd.DataFrame.from_dict(result)
    elif function == "stock_hist_data_singelday":
        df = pd.Series(j).to_frame().T.drop("status", axis=1).set_index("from")
    elif function == "stock_hist_data_orderflow":
        result = j['results']
        df = pd.DataFrame.from_dict(result)

    while "next_url" in j.keys():
        next_url = j["next_url"]+"&apiKey="+api_keys.poly_dict["api_key"]
        r = requests.get(next_url)
        j = r.json()
        df_temp = pd.DataFrame.from_dict(j["results"])
        df = pd.concat([df,df_temp])
    return df

def get_10y_trading(ticker="IPAR"):
    input_dict = {}
    input_dict["ticker"] = ticker
    input_dict['function'] = "stock_hist_data"
    input_dict['t0']="2011/01/01"
    input_dict['tn']="2021/01/01"
    input_dict["timeintervalscale"] = 1
    input_dict["timeinterval"] = "day"
    input_dict['price_adj']= True
    input_dict['sortAD'] = True
    input_dict['limit'] = 50000
    df = get_df_poly(input_dict)
    df["index"] = 0
    df["index"] = df["t"].apply(lambda x: unix_time_to_datetime(x))
    df = df.set_index(df["index"]).drop("index", axis=1)
    return df

def financials_clean():
    """
    NOT ACTIVE. THIS IS AS REPORTED, different from ticker to ticker, and from time to time. :(
    """
    ticker= "BG"
    url = url_financials(ticker, timeframe_FS="annual", include_sourcesTF=True, sortAD=True, limit=100, sortRF=True)
    r = requests.get(url)
    j = r.json()
    result_lc=j["results"]
    for i in range(len(result_lc)):
        ref_dict = {k: result_lc[i][k] for k in ['start_date', 'end_date', 'filing_date', 'cik', 'company_name', 'fiscal_period', 'fiscal_year']}
        financials_dict =  result_lc[i]['financials']
        comprehensive_income_dict = financials_dict['comprehensive_income']
        comprehensive_income_df= pd.DataFrame()
        for item in comprehensive_income_dict:
            comprehensive_income_df[item] = [comprehensive_income_dict[item]["value"]]
        income_statement_dict = financials_dict['income_statement']

def get_us_exchanges():
    exchange_df = pd.read_csv(r"C:\Users\Peter Yan\Desktop\repo\peterzergquant\Data\ISO10383_MIC.csv")
    us_exchange_df = exchange_df[(exchange_df["COUNTRY"]=="UNITED STATES OF AMERICA")&(exchange_df["STATUS"]=="ACTIVE")]
    MIC_lc = us_exchange_df["MIC"].tolist()
    return MIC_lc

def get_all_ticker_in_exchange(exchange_MIC):
    url = url_tickers(exchange_MIC)
    next_url = None
    r = requests.get(url)
    j = r.json()
    #check status
    if j['status']!="OK":
        print("status: "+j['status'])
        return None
    if "next_url" in j.keys():
        next_url = j["next_url"]

    ticker_lc = [d['ticker'] for d in j["results"] if 'ticker' in d]
    while "next_url" in j.keys():
        next_url = j["next_url"]+"&apiKey="+api_keys.poly_dict["api_key"]
        r = requests.get(next_url)
        j = r.json()
        ticker_lc += [d['ticker'] for d in j["results"] if 'ticker' in d]
    return ticker_lc

def get_all_ticker_in_us(full=False):
    if full==True:
        MIC_lc = get_us_exchanges()
    else:
        MIC_lc = ["XNAS","XNYS"]
    lc = []
    for mic in MIC_lc:
        lc += get_all_ticker_in_exchange(mic)
    return lc

def get_all_liquid_tickers_in_us():
    ticker_lc = get_all_ticker_in_us()
    output_lc = filter_ticker(ticker_lc)
    return output_lc

def save_tickers(output_lc):
    df = pd.DataFrame(output_lc)
    file_path = r"C:\Users\Peter Yan\Desktop\repo\peterzergquant\Data\all_liquid_ticker"
    feather.write_feather(df, file_path)
    print("file output done")


polygon_system_setup()

def test():
    ticker = "AAPL"
    url = url_ticker_info(ticker)
    r = requests.get(url)
    j = r.json()
    d = j["results"]
    return d

