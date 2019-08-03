from alpha_api_parser import api_parser
import sqlite3
from db_init import create_connection
import logging
import os
from time import sleep
import datetime

logging.basicConfig(level=logging.INFO)

def main():
    
    # setting up api configs
    execution_date = datetime.datetime.now()
    base_url = 'https://www.alphavantage.co/query?function={}&symbol={}&market={}&apikey={}'
    function = 'DIGITAL_CURRENCY_DAILY'
    symbol = 'BTC'
    market = 'CNY'
    apikey = 'demo'
    table_name = 'crypto_daily'
    sqlite_db = 'db/crypto.db'
    archive_folder = 'archives'
    sleep_seconds = 5

    #parent dictionary key for dataframe
    parent_dict_key = 'Time Series (Digital Currency Daily)'

    #custom columns list
    df_column_list = ['open_cny','open_usd','high_cny','high_usd','low_cny','low_usd','close_cny','close_usd','volume','market_cap','date', 'coin']

    #using custom module calling api_parser
    df = api_parser(base_url, function, symbol, market, apikey, parent_dict_key, df_column_list)


    #creating the directory structure
    if not os.path.exists(archive_folder + '/' + symbol):
        os.makedirs(archive_folder + '/' + symbol)

    logging.info("Archiving the data")
    sleep(sleep_seconds)
    #archiving the file
    df.to_csv(archive_folder + '/' + symbol + '/' + str(execution_date) + '.csv', index=False)

    #setting up db 
    logging.info("Setting up database connection")
    sleep(sleep_seconds)
    conn = create_connection(sqlite_db)

    logging.info("Saving the data in database table")
    sleep(sleep_seconds)
    #saving the df to db table
    df.to_sql(table_name, conn, if_exists='replace', index=False)

    logging.info("Testing the data by getting a sample of it")
    sleep(sleep_seconds)
    test_sample_data(conn, table_name)

    logging.info("Terminating!!")
    sleep(sleep_seconds)


def test_sample_data(conn, table_name):
    cursorObj = conn.cursor()
    cursorObj.execute('SELECT * FROM '+table_name+' limit 2')
 
    rows = cursorObj.fetchall()
 
    for row in rows:
        print(row)


if __name__ == '__main__':
    main()