import requests
import pandas as pd
import api_keys

pd.options.mode.chained_assignment = None

#set up, hard code
base_func_alpha = "https://www.alphavantage.co/query?function="
field_dict_alpha = {}
req_dict_alpha = {}
field_value_dict_alpha = {}
default_value_dict_alpha = {}
dict_LC_alpha = [field_dict_alpha, req_dict_alpha, field_value_dict_alpha, default_value_dict_alpha]

#system data input
def alphavantage_system_set_up(setup_lc = [1,1,1]):
    global dict_LC_alpha
    global field_dict_alpha
    global req_dict_alpha
    global field_value_dict_alpha
    global default_value_dict_alpha
    #alphavantage_financials_set_up() if setup_lc[0]==1 else None
    alphavantage_econ_set_up() if setup_lc[1]==1 else None
    alphavantage_market_set_up() if setup_lc[2]==1 else None

def alphavantage_financials_set_up():
    global dict_LC_alpha
    global field_dict_alpha
    global req_dict_alpha
    global field_value_dict_alpha
    global default_value_dict_alpha
    financials_set_up()

def alphavantage_econ_set_up():
    global dict_LC_alpha
    global field_dict_alpha
    global req_dict_alpha
    global field_value_dict_alpha
    global default_value_dict_alpha
    macro_set_up() #causin bugs for single stock

def alphavantage_market_set_up():
    global dict_LC_alpha
    global field_dict_alpha
    global req_dict_alpha
    global field_value_dict_alpha
    global default_value_dict_alpha
    market_data_set_up()
    ma_set_up()
    conv_set_up()
    trend_strength_set_up()
    vix_set_up()

def market_data_set_up():
    global dict_LC_alpha
    global field_dict_alpha
    global req_dict_alpha
    global field_value_dict_alpha
    global default_value_dict_alpha
    # field value
    field_value_dict_alpha["outputsize"]=['compact', 'full']
    # default value
    default_value_dict_alpha["datatype"] = 'json'
    default_value_dict_alpha["outputsize"] = 'compact'
    #function
    LC = ["TIME_SERIES_DAILY"]
    for function in LC:
        field_dict_alpha[function] = ["function", "symbol",
                                "outputsize", "datatype",
                                "apikey"]
        field_LC = field_dict_alpha[function]
        req_LC = [1, 1, 0, 0, 1]
        for i in range(len(field_LC)):
            req_dict_alpha[(function, field_LC[i])] = req_LC[i]

def ma_set_up():
    global dict_LC_alpha
    global field_dict_alpha
    global req_dict_alpha
    global field_value_dict_alpha
    global default_value_dict_alpha
    #field value
    field_value_dict_alpha["interval"] = ['1min', '5min', '15min', '30min', '60min', 'daily', 'weekly', 'monthly']
    field_value_dict_alpha["series_type"] = ['close', 'open', 'high', 'low']
    field_value_dict_alpha["datatype"] = ['json', 'csv']
    #default value
    default_value_dict_alpha["function"] = "SMA"
    default_value_dict_alpha["symbol"] = "BG"
    default_value_dict_alpha["interval"] = 'daily'
    default_value_dict_alpha["time_period"] = '30'
    default_value_dict_alpha["series_type"] = 'close'
    default_value_dict_alpha["fastlimit"] = '0.01'
    default_value_dict_alpha["slowlimit"] = '0.01'
    default_value_dict_alpha["apikey"] = 'LTPK0059ECO8ROZ4'
    # ma
    ma_LC = ["SMA","EMA","WMA","DEMA","TEMA","TRIMA","KAMA","T3"]
    for function in ma_LC:
        field_dict_alpha[function] = ["function", "symbol", "interval",
                                "time_period","series_type",
                                "datatype","apikey"]
        field_LC = field_dict_alpha[function]
        ma_req_LC = [1, 1, 1, 1, 1, 0, 1]
        for i in range(len(field_LC)):
            req_dict_alpha[(function, field_LC[i])] = ma_req_LC[i]
    # mama
    function = "MAMA"
    field_dict_alpha[function] = ["function", "symbol", "interval", "series_type",
                            "fastlimit","slowlimit",
                            "datatype","apikey"]
    ma_LC = field_dict_alpha[function]
    ma_req_LC = [1, 1, 1, 1, 0, 0, 0, 1]
    for i in range(len(ma_req_LC)):
        req_dict_alpha[(function, ma_LC[i])] = ma_req_LC[i]

def conv_set_up():
    global dict_LC_alpha
    global field_dict_alpha
    global req_dict_alpha
    global field_value_dict_alpha
    global default_value_dict_alpha
    #field value
    field_value_dict_alpha["fastmatype"] = ['0', '1', '2', '3', '4', '5', '6', '7', '8']
    field_value_dict_alpha["slowmatype"] = ['0', '1', '2', '3', '4', '5', '6', '7', '8']
    field_value_dict_alpha["signalmatype"] = ['0', '1', '2', '3', '4', '5', '6', '7', '8']
    #default value
    default_value_dict_alpha["time_period"] = '10' #stochrsi
    default_value_dict_alpha["fastkperiod"] = '5' #stochrsi
    default_value_dict_alpha["fastdperiod"] = '3' #stochrsi
    default_value_dict_alpha["fastdmatype"] = '0' #stochrsi
    default_value_dict_alpha["fastperiod"]= '12'
    default_value_dict_alpha["slowperiod"]= '26'
    default_value_dict_alpha["signalperiod"]= '9'
    default_value_dict_alpha["fastmatype"]= '0'
    default_value_dict_alpha["slowmatype"]= '0'
    default_value_dict_alpha["signalmatype"]= '0'
    default_value_dict_alpha["matype"]= '0'
    #Integers 0 - 8 are accepted with the following mappings.
    # 0 = Simple Moving Average (SMA),
    # 1 = Exponential Moving Average (EMA),
    # 2 = Weighted Moving Average (WMA),
    # 3 = Double Exponential Moving Average (DEMA),
    # 4 = Triple Exponential Moving Average (TEMA),
    # 5 = Triangular Moving Average (TRIMA),
    # 6 = T3 Moving Average,
    # 7 = Kaufman Adaptive Moving Average (KAMA),
    # 8 = MESA Adaptive Moving Average (MAMA).
    #convergence
    conv_LC = ["MACDEXT"]
    for function in conv_LC:
        field_dict_alpha[function] = ["function", "symbol", "interval", "series_type",
                                "fastperiod","slowperiod","signalperiod","fastmatype","slowmatype","signalmatype",
                                "datatype","apikey"]
        field_LC = field_dict_alpha[function]
        conv_req_LC = [1,1,1,1,0,0,0,0,0,0,1]
        for i in range(len(conv_req_LC)):
            req_dict_alpha[(function, field_LC[i])] = conv_req_LC[i]
    #converge but special:
    conv_LC = ["STOCHRSI"]
    for function in conv_LC:
        field_dict_alpha[function] = ["function", "symbol", "interval", "time_period", "series_type",
                                "fastkperiod","fastdperiod", "fastdmatype",
                                "datatype", "apikey"]
        field_LC = field_dict_alpha[function]
        conv_req_LC = [1, 1, 1, 1, 1, 0, 0, 0, 0, 1]
        for i in range(len(conv_req_LC)):
            req_dict_alpha[(function, field_LC[i])] = conv_req_LC[i]
    #RSI
    rsi_LC = ["RSI","CMO"]
    for function in rsi_LC:
        field_dict_alpha[function] = ["function", "symbol", "interval", "time_period", "series_type",
                                "datatype", "apikey"]
        field_LC = field_dict_alpha[function]
        conv_req_LC = [1, 1, 1, 1, 1, 0, 1]
        for i in range(len(conv_req_LC)):
            req_dict_alpha[(function, field_LC[i])] = conv_req_LC[i]
    #MACD but varied price oscillator
    function = "APO"
    field_dict_alpha[function] = ["function", "symbol", "interval", "series_type",
                            "fastperiod", "slowperiod","matype","datatype",
                            "apikey"]
    field_LC = field_dict_alpha[function]
    conv_req_LC = [1, 1, 1, 1, 0,0,0,0, 1]
    for i in range(len(conv_req_LC)):
        req_dict_alpha[(function, field_LC[i])] = conv_req_LC[i]

def trend_strength_set_up():
    global dict_LC_alpha
    global field_dict_alpha
    global req_dict_alpha
    global field_value_dict_alpha
    global default_value_dict_alpha
    #default
    default_value_dict_alpha["time_period1"]= "7"
    default_value_dict_alpha["time_period2"]= "14"
    default_value_dict_alpha["time_period3"]= "28"
    default_value_dict_alpha["acceleration"]= "0.01"
    default_value_dict_alpha["maximum"]= "0.2"
    #overbuy/oversold, trend strength
    strenth_LC = ["WILLR","ADX","ADXR","CCI","AROON","AROONOSC","MFI","DX",
                  "MINUS_DI","PLUS_DI","MINUS_DM","PLUS_DM"]
    for function in strenth_LC:
        field_dict_alpha[function] = ["function", "symbol", "interval", "time_period",
                                "datatype", "apikey"]
        field_LC = field_dict_alpha[function]
        conv_req_LC = [1, 1, 1, 1, 0, 1]
        for i in range(len(conv_req_LC)):
            req_dict_alpha[(function, field_LC[i])] = conv_req_LC[i]
    #muti-period overbought
    mutistrenth_LC = ["ULTOSC"]
    for function in mutistrenth_LC:
        field_dict_alpha[function] = ["function", "symbol", "interval",
                                "time_period1","time_period2","time_period3",
                                "datatype", "apikey"]
        field_LC = field_dict_alpha[function]
        conv_req_LC = [1,1,1, 0,0,0, 0,1]
        for i in range(len(conv_req_LC)):
            req_dict_alpha[(function, field_LC[i])] = conv_req_LC[i]
    #acceleration
    speeddelta_LC = ["MOM","ROC","ROCR","TRIX"]
    for function in speeddelta_LC:
        field_dict_alpha[function] = ["function", "symbol", "interval", "time_period", "series_type",
                                "datatype", "apikey"]
        field_LC = field_dict_alpha[function]
        conv_req_LC = [1, 1, 1, 1, 1, 0, 1]
        for i in range(len(conv_req_LC)):
            req_dict_alpha[(function, field_LC[i])] = conv_req_LC[i]
    #buyer/seller strength
    balance_LC = ["BOP","TRANGE"]
    for function in balance_LC:
        field_dict_alpha[function] = ["function", "symbol", "interval",
                                "datatype", "apikey"]
        field_LC = field_dict_alpha[function]
        conv_req_LC = [1, 1, 1, 0, 1]
        for i in range(len(conv_req_LC)):
            req_dict_alpha[(function, field_LC[i])] = conv_req_LC[i]
    #trailing stop
    trailing_LC = ["SAR"]
    for function in trailing_LC:
        field_dict_alpha[function] = ["function", "symbol", "interval",
                                "acceleration","maximum",
                                "datatype", "apikey"]
        field_LC = field_dict_alpha[function]
        conv_req_LC = [1, 1, 1,0,0,0,1]
        for i in range(len(conv_req_LC)):
            req_dict_alpha[(function, field_LC[i])] = conv_req_LC[i]

def vix_set_up():
    global dict_LC_alpha
    global field_dict_alpha
    global req_dict_alpha
    global field_value_dict_alpha
    global default_value_dict_alpha
    #vix related function
    voli_LC = ["ATR","NATR"]
    for function in voli_LC:
        field_dict_alpha[function] = ["function", "symbol", "interval", "time_period",
                                "datatype", "apikey"]
        field_LC = field_dict_alpha[function]
        conv_req_LC = [1,1,1,1,0,1]
        for i in range(len(conv_req_LC)):
            req_dict_alpha[(function, field_LC[i])] = conv_req_LC[i]
    #volumn
    volumn_LC = ["AD","ADOSC","OBV"]
    for function in volumn_LC:
        field_dict_alpha[function] = ["function", "symbol", "interval",
                                "datatype", "apikey"]
        field_LC = field_dict_alpha[function]
        conv_req_LC = [1, 1, 1,0,1]
        for i in range(len(conv_req_LC)):
            req_dict_alpha[(function, field_LC[i])] = conv_req_LC[i]
            
def macro_set_up():
    global dict_LC_alpha
    global field_dict_alpha
    global req_dict_alpha
    global field_value_dict_alpha
    global default_value_dict_alpha
    default_value_dict_alpha["apikey"] = 'LTPK0059ECO8ROZ4'
    #reasury yield
    yield_LC = ["TREASURY_YIELD"]
    default_value_dict_alpha["maturity"]= "30year"
    field_value_dict_alpha["maturity"]=['3month','2year','5year','7year','10year','30year']
    for function in yield_LC:
        field_dict_alpha[function] = ["function",
                                      "interval", "maturity","datatype","apikey"]
        field_LC = field_dict_alpha[function]
        conv_req_LC = [1,0,0,0,1]
        for i in range(len(conv_req_LC)):
            req_dict_alpha[(function, field_LC[i])] = conv_req_LC[i]
    #FEDERAL_FUNDS_RATE
    fedrate_LC = ["FEDERAL_FUNDS_RATE"]
    for function in fedrate_LC:
        field_dict_alpha[function] = ["function",
                                      "interval","datatype",
                                      "apikey"]
        field_LC = field_dict_alpha[function]
        conv_req_LC = [1,0,0,1]
        for i in range(len(conv_req_LC)):
            req_dict_alpha[(function, field_LC[i])] = conv_req_LC[i]
    #other
    other_LC = ["INFLATION","INFLATION_EXPECTATION","CONSUMER_SENTIMENT","RETAIL_SALES","DURABLES","UNEMPLOYMENT","NONFARM_PAYROLL"]
    for function in other_LC:
        field_dict_alpha[function] = ["function",
                                      "datatype",
                                      "apikey"]
        field_LC = field_dict_alpha[function]
        conv_req_LC = [1,0,1]
        for i in range(len(conv_req_LC)):
            req_dict_alpha[(function, field_LC[i])] = conv_req_LC[i]

#force dictionary to be ordered
def force_reorder(raw_dict):
    key_lc = list(raw_dict.keys())
    if len(key_lc)>1:
        key_lc.sort()
        reordered_dict = {k: raw_dict[k] for k in key_lc}
    return reordered_dict

#ref functions
def financials_set_up():
    global dict_LC_alpha
    global field_dict_alpha
    global req_dict_alpha
    global field_value_dict_alpha
    global default_value_dict_alpha
    default_value_dict_alpha["apikey"] = 'LTPK0059ECO8ROZ4'
    # reasury yield
    function_LC = ["INCOME_STATEMENT","BALANCE_SHEET","CASH_FLOW"]#"EARNINGS","OVERVIEW"
    for function in function_LC:
        field_dict_alpha[function] = ["function","symbol","apikey"]
        field_LC = field_dict_alpha[function]
        req_LC = [1, 1, 1]
        for i in range(len(req_LC)):
            req_dict_alpha[(function, field_LC[i])] = req_LC[i]

########################################################################################################################
#action
#url function
def get_url(input_dict):
    if "function" in input_dict:
        function = input_dict["function"]
    else:
        function = default_value_dict_alpha["function"]
    # get fields
    field_LC = field_dict_alpha[function]
    url = base_func_alpha + function
    for field in field_LC[1:]:  # skip function and ticker
        if field in input_dict:
            if field in field_value_dict_alpha:
                if str(input_dict[field]) in field_value_dict_alpha[field]:
                    temp_value = str(input_dict[field])
                else:
                    print("ERROR: invalid DIY field input, replaced with default.")
                    temp_value = str(default_value_dict_alpha[field])
            else:
                temp_value = str(input_dict[field])
        else:
            temp_value = str(default_value_dict_alpha[field])
        temp_str = "&" + str(field) + "=" + temp_value
        url += temp_str
    return url

#r to def function for market
def from_request_to_df(r):
    j = r.json()
    yield_name_LC=["3-Month Treasury Constant Maturity Rate","30-Year Treasury Constant Maturity Rate"]
    if ("name" in j.keys()) and j["name"] in yield_name_LC:
        d=j["data"]
        df = pd.DataFrame.from_dict(d)
        df.index = pd.to_datetime(df.index)
        df.set_index(df["date"],inplace=True)
        df.drop("date",axis=1,inplace=True)
    else:
        for key in j:
            if key!='Meta Data':
                d=j[key]
                keys = d.keys()
                #force sort dictionary (later df col order)
                if len(list(d[list(keys)[0]].keys()))>1:
                    for date_index in list(keys):
                        d[date_index] = force_reorder(d[date_index])
        df = pd.DataFrame.from_dict(d).transpose()
        df.index = pd.to_datetime(df.index)
    return df

def get_df_market(input_dict):
    url = get_url(input_dict)
    r = requests.get(url)
    df = from_request_to_df(r)
    return df
    #print(df.to_markdown())

def get_df_financials(input_dict):
    url = get_url(input_dict)
    r = requests.get(url)
    j = r.json()
    annual_df = pd.DataFrame(j["annualReports"])
    quarterly_df = pd.DataFrame(j["quarterlyReports"])
    return annual_df, quarterly_df

def get_df_all_financials():
    """
    :return: a dictionary of financials, pair based keys
    """
    ticker="BG"
    input_dict = {}
    input_dict["symbol"]=ticker
    financials_dict = {}
    for function in field_dict_alpha:
        print(function)
        input_dict["function"] = function
        annual_df, quarterly_df = get_df_financials(input_dict)
        financials_dict[function,"annual"] = annual_df
        financials_dict[function,"quarterly"] = quarterly_df
    return financials_dict
#other
def ticker_matching(ticker):
    """
    :param ticker: ticker guess
    :return: df of tickers, col: '1. symbol', '2. name', '3. type', '4. region', '5. marketOpen',
       '6. marketClose', '7. timezone', '8. currency', '9. matchScore'
    """
    url = "https://www.alphavantage.co/query?function=SYMBOL_SEARCH&keywords="+ticker+"&apikey="+api_keys.alpha_dict["api_key"]
    r = requests.get(url)
    j = r.json()
    df = pd.DataFrame.from_dict(j["bestMatches"])
    return df

#test



alphavantage_system_set_up(setup_lc = [0,0,1])