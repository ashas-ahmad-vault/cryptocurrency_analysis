import requests
import json
import pandas as pd

def api_parser(base_url = 'https://www.alphavantage.co/query?function={}&symbol={}&market={}&apikey={}', function = 'DIGITAL_CURRENCY_DAILY', symbol = 'BTC', market = 'CNY', apikey = 'demo', parent_dict_key = 'Time Series (Digital Currency Daily)', column_list = ['open_cny','open_usd','high_cny','high_usd','low_cny','low_usd','close_cny','close_usd','volume','market_cap','date']):
    # This function is responsible
    # of taking alpha advantage api configs
    # and returning a pandas dataframe

    response = requests.get(base_url.format(function, symbol, market, apikey))
    data_dict = json.loads(response.text)

    df = pd.DataFrame(data_dict[parent_dict_key])
    df = df.transpose()

    
    # setting up two more columns in dataframe
    # of date and coin 
    df['date'] = df.index
    df['coin'] = symbol
    
    #renaming column headers
    df.columns = column_list
    
    return df