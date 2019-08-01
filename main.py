from alpha_api_parser import api_parser

def main():
    # setting up api configs
    base_url = 'https://www.alphavantage.co/query?function={}&symbol={}&market={}&apikey={}'
    function = 'DIGITAL_CURRENCY_DAILY'
    symbol = 'BTC'
    market = 'CNY'
    apikey = 'demo'

    #parent dictionary key for dataframe
    parent_dict_key = 'Time Series (Digital Currency Daily)'

    #custom columns list
    df_column_list = ['open_cny','open_usd','high_cny','high_usd','low_cny','low_usd','close_cny','close_usd','volume','market_cap','date', 'coin']

    #using custom module calling api_parser
    df = api_parser(base_url, function, symbol, market, apikey, parent_dict_key, df_column_list)
    
    print(df.head(10))


def save_in_db():
    

if __name__ == '__main__':
    main()