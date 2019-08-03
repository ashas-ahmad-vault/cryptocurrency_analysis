from alpha_api_parser import api_parser
from db_init import create_connection, test_sample_data, save_result
import sqlite3
import pandas as pd
import os
import json
from time import sleep
import datetime
import logging
import relative_span

logging.basicConfig(level=logging.INFO)

def handler():
    
    # setting up api configs
    execution_date = datetime.datetime.now()
    base_url = 'https://www.alphavantage.co/query?function={}&symbol={}&market={}&apikey={}'
    function = 'DIGITAL_CURRENCY_DAILY'
    symbol = 'BTC'
    market = 'CNY'
    apikey = 'demo'
    table_name = 'crypto_daily'
    result_tablename = 'maxspan'
    sqlite_db = 'crypto.db'
    archive_folder = 'archives'
    sleep_seconds = 0
    start_year = 2014
    end_year = datetime.datetime.now().year

    try:
        # parent dictionary key for dataframe
        parent_dict_key = 'Time Series (Digital Currency Daily)'

        # custom columns list
        df_column_list = ['open_cny','open_usd','high_cny','high_usd','low_cny','low_usd','close_cny','close_usd','volume','market_cap','date', 'coin']

        # using custom module calling api_parser
        df = api_parser(base_url, function, symbol, market, apikey, parent_dict_key, df_column_list)


        # creating the directory structure
        if not os.path.exists(archive_folder + '/' + symbol):
            os.makedirs(archive_folder + '/' + symbol)

        logging.info("Archiving the data")
        sleep(sleep_seconds)
        # archiving the file
        df.to_csv(archive_folder + '/' + symbol + '/' + str(execution_date) + '.csv', index=False)

        # setting up db 
        logging.info("Setting up database connection")
        sleep(sleep_seconds)
        conn = create_connection(sqlite_db)

        logging.info("Saving the data in database table")
        sleep(sleep_seconds)
        # saving the df to db table
        df.to_sql(table_name, conn, if_exists='replace', index=False)

        logging.info(str(len(df)) + " records have been inserted in " + table_name)
        sleep(sleep_seconds)

        logging.info("Testing the data by getting a sample of it")
        sleep(sleep_seconds)
        records = test_sample_data(conn, table_name)

        # printing out sample records
        for record in records:
            print (record)


        logging.info("Reading the table into dataframe")
        sleep(sleep_seconds)
        db_df = pd.read_sql_query("SELECT date, close_usd FROM " + table_name, conn)

        # setting up index and sorting
        db_df = db_df.set_index(['date'])
        db_df.sort_index(inplace=True, ascending=True)

        logging.info("Reading Successfull")
        sleep(sleep_seconds)

        # calculating weekly relative spans of closing price
        logging.info("Calculating relative spans")
        sleep(sleep_seconds)
        weekly_spans = relative_span.get_relative_spans(start_year, end_year, db_df)
        greatest_span_key = relative_span.get_week_with_greatest_relative_span(weekly_spans)

        greatest_span = weekly_spans[greatest_span_key]
        result_dict = {}
        result_dict[greatest_span_key] = weekly_spans[greatest_span_key]

        print(greatest_span_key + ' had the greatest relative span for ' + symbol + ' with value of ' + str(greatest_span))

        # saving the results in db table for API use
        save_result(conn, result_tablename, [greatest_span_key, result_dict[greatest_span_key], execution_date])

        return json.dumps(result_dict)
        
    except Exception:
        logging.exception("message")
    finally:
        conn.close()

if __name__ == '__main__':
    print(handler())
    logging.info("Terminating!!")