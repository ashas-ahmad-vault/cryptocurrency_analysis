import sqlite3
from sqlite3 import Error
import logging
from time import sleep

logging.basicConfig(level=logging.INFO)
sleep_seconds = 5
 
 
def create_connection(db_file):
    """ create a database connection to a SQLite database """
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)
    return conn

def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)

def test_sample_data(conn, table_name):
    cursorObj = conn.cursor()
    cursorObj.execute('SELECT * FROM '+table_name+' limit 2')
 
    rows = cursorObj.fetchall()

    return rows

def save_result(conn, table_name, list_items):
    conn.execute('insert into '+table_name+' values (?,?,?)', list_items)
    conn.commit()

def get_maxspan(conn, table_name):
    cursorObj = conn.cursor()
    cursorObj.execute('SELECT * FROM '+table_name+' order by load_date desc limit 1')
 
    rows = cursorObj.fetchall()

    return rows

def main():
    database = "crypto.db"
 
    sql_create_projects_table = """ CREATE TABLE IF NOT EXISTS crypto_daily (
                                        open_cny decimal,
                                        open_usd decimal,
                                        high_cny decimal,
                                        high_usd decimal,
                                        low_cny decimal,
                                        low_usd decimal,
                                        close_cny decimal,
                                        close_usd decimal,
                                        volume decimal,
                                        market_cap decimal,
                                        date date PRIMARY KEY,
                                        coin text
                                    ); """

    sql_create_maxspans_table = """ CREATE TABLE IF NOT EXISTS maxspan (
                                    week_year decimal,
                                    relative_span decimal,
                                    load_date timestamp
                                ); """

    logging.info("Going to create sqlite3 database "+database+" if it does not exists")
    sleep(sleep_seconds)

    # create a database connection
    conn = create_connection(database)
    logging.info("Created Successfully")
    sleep(sleep_seconds)

    if conn is not None:
        # create data table
        logging.info("Going to create table crypto_daily if it does not exists")
        sleep(sleep_seconds)
        create_table(conn, sql_create_projects_table)

        # create result table
        logging.info("Going to create table maxspan for result if it does not exists")
        create_table(conn, sql_create_maxspans_table)
        logging.info("Created Successfully")
        sleep(sleep_seconds)
    else:
        print("Error! cannot create the database connection.")

    logging.info("Terminating!!")
    sleep(sleep_seconds)

 
if __name__ == '__main__':
    main()
    