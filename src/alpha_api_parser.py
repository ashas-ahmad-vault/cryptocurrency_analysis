import requests
import json
import pandas as pd
import logging
from time import sleep

sleep_in_seconds = 5

logging.basicConfig(level=logging.INFO)

def api_parser(base_url, function, symbol, market, apikey, parent_dict_key, column_list):
    # This function is responsible
    # of taking alpha advantage api configs
    # and returning a pandas dataframe

    
    logging.info("Going to make the API request")
    sleep(sleep_in_seconds)
    response = requests.get(base_url.format(function, symbol, market, apikey))
    if response.status_code != 200:
        logging.error("Warning : Status Code call <> 200: {}".format(res.status_code))

    else:
        logging.info("Request is successful")
        sleep(sleep_in_seconds)
        data_dict = json.loads(response.text)

        df = pd.DataFrame(data_dict[parent_dict_key])
        df = df.transpose()

    
        # setting up two more columns in dataframe
        # of date and coin 
        logging.info("Setting up date and coin columns")
        sleep(2)
        df['date'] = df.index
        df['coin'] = symbol

        #renaming column headers
        df.columns = column_list
    
    return df