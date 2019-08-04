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


class CryptoCurrency:
    def __init__(self):
        self.execution_date = datetime.datetime.now()
        self.base_url = "https://www.alphavantage.co/query?function={}&symbol={}&market={}&apikey={}"
        self.function = "DIGITAL_CURRENCY_DAILY"
        self.symbol = "BTC"
        self.market = "CNY"
        self.apikey = "demo"
        self.table_name = "crypto_daily"
        self.result_tablename = "maxspan"
        self.sqlite_db = "crypto.db"
        self.archive_folder = "archives"
        self.sleep_seconds = 5
        self.start_year = 2014
        self.end_year = datetime.datetime.now().year
        self.parent_dict_key = "Time Series (Digital Currency Daily)"
        self.df_column_list = [
            "open_cny",
            "open_usd",
            "high_cny",
            "high_usd",
            "low_cny",
            "low_usd",
            "close_cny",
            "close_usd",
            "volume",
            "market_cap",
            "date",
            "coin",
        ]

        CryptoCurrency.handler(self)

    @classmethod
    def log_info(cls, message, seconds):
        logging.info(message)
        sleep(seconds)

    @classmethod
    def handler(cls, self):
        try:
            # using custom module calling api_parser
            df = api_parser(
                self.base_url,
                self.function,
                self.symbol,
                self.market,
                self.apikey,
                self.parent_dict_key,
                self.df_column_list,
            )

            # creating the directory structure
            if not os.path.exists(self.archive_folder + "/" + self.symbol):
                os.makedirs(self.archive_folder + "/" + self.symbol)

            cls.log_info("Archiving the data", self.sleep_seconds)
            # archiving the file
            df.to_csv(
                self.archive_folder
                + "/"
                + self.symbol
                + "/"
                + str(self.execution_date)
                + ".csv",
                index=False,
            )

            # setting up db
            cls.log_info("Setting up database connection", self.sleep_seconds)
            conn = create_connection(self.sqlite_db)

            cls.log_info("Saving the data in database table", self.sleep_seconds)
            # saving the df to db table
            df.to_sql(self.table_name, conn, if_exists="replace", index=False)

            cls.log_info(
                str(len(df)) + " records have been inserted in " + self.table_name,
                self.sleep_seconds,
            )

            cls.log_info(
                "Testing the data by getting a sample of it", self.sleep_seconds
            )
            records = test_sample_data(conn, self.table_name)

            # printing out sample records
            for record in records:
                print(record)

            cls.log_info("Reading the table into dataframe", self.sleep_seconds)
            db_df = pd.read_sql_query(
                "SELECT date, close_usd FROM " + self.table_name, conn
            )

            # setting up index and sorting
            db_df = db_df.set_index(["date"])
            db_df.sort_index(inplace=True, ascending=True)
            cls.log_info("Reading Successfull", self.sleep_seconds)

            # calculating weekly relative spans of closing price
            cls.log_info("Calculating relative spans", self.sleep_seconds)
            weekly_spans = relative_span.get_relative_spans(
                self.start_year, self.end_year, db_df
            )
            greatest_span_key = relative_span.get_week_with_greatest_relative_span(
                weekly_spans
            )

            greatest_span = weekly_spans[greatest_span_key]
            result_dict = {}
            result_dict[greatest_span_key] = weekly_spans[greatest_span_key]

            print(
                greatest_span_key
                + " had the greatest relative span for "
                + self.symbol
                + " with value of "
                + str(greatest_span)
            )

            # saving the results in db table for API use
            cls.log_info(
                "Storing the result in results maxspan table", self.sleep_seconds
            )
            save_result(
                conn,
                self.result_tablename,
                [
                    greatest_span_key,
                    result_dict[greatest_span_key],
                    self.execution_date,
                ],
            )

            return json.dumps(result_dict)

        except Exception:
            logging.exception("message")
        finally:
            conn.close()


if __name__ == "__main__":
    CryptoCurrency()
    logging.info("Terminating!!")
