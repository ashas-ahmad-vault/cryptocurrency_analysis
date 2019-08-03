from alpha_api_parser import api_parser
from db_init import create_connection
import sqlite3
import pandas as pd
import os
from time import sleep
import datetime
import logging

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
    sqlite_db = 'crypto.db'
    archive_folder = 'archives'
    sleep_seconds = 0
    start_year = 2014
    end_year = datetime.datetime.now().year

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

    logging.info(str(len(df)) + " records have been inserted in " + table_name)
    sleep(sleep_seconds)

    logging.info("Testing the data by getting a sample of it")
    sleep(sleep_seconds)
    test_sample_data(conn, table_name)

    get_relative_spans(start_year, end_year, df, conn, table_name)

    logging.info("Terminating!!")
    sleep(sleep_seconds)

def get_relative_spans(start_year, end_year,  df, conn, table_name):
    relative_span_dict = {}
    week_counter = 1

    df = pd.read_sql_query("SELECT * FROM " + table_name, conn)
    df = df.set_index(['date'])
    df.sort_index(inplace=True, ascending=True)

    for year in range(start_year, end_year+1):
        total_weeks = weeks_for_year(year)
        for week in range(1, total_weeks + 1):
            start, end = get_start_end_dates(year,week)
            print(df.loc[start:end])


def test_sample_data(conn, table_name):
    cursorObj = conn.cursor()
    cursorObj.execute('SELECT * FROM '+table_name+' limit 2')
 
    rows = cursorObj.fetchall()
 
    for row in rows:
        print(row)

def get_start_end_dates(year, week):
     d = datetime.date(year,1,1)
     if(d.weekday()<= 3):
         d = d - datetime.timedelta(d.weekday())             
     else:
         d = d + datetime.timedelta(7-d.weekday())
     dlt = datetime.timedelta(days = (week-1)*7)
     return str(d + dlt),  str(d + dlt + datetime.timedelta(days=6))

def weeks_for_year(year):
    last_week = datetime.date(int(year), 12, 28)
    return last_week.isocalendar()[1]

if __name__ == '__main__':
    main()