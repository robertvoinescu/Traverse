import json
import os
import sys
import time
import math
from datetime import datetime
import pytz
import argparse

import adal
import numpy as np
import pandas as pd
import requests
import logging

sys.path.insert(
    0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
)


AUTHORITY_HOST_URL = "https://login.microsoftonline.com"

script_path = os.path.realpath(__file__)
script_folder = os.path.realpath(os.path.join(script_path, ".."))

with open(os.path.join(script_folder, "stream_credentials.json")) as f:
    creds = json.load(f)

API_ENDPOINT = creds["api_endpoint"]
API_PING_ENDPOINT = creds["api_ping_endpoint"]
CLIENT_SECRET = creds["secret"]
RESOURCE = creds["resource"]
CLIENT_ID = creds["client_id"]
TENANT = creds["tenant"]
AUTHORITY_URL = f"{AUTHORITY_HOST_URL}/{TENANT}"

iso_energy_products = {
    "caiso": ["energy_da", "energy_rt_5", "energy_rt_15"],
    "ercot": ["energy_da", "energy_rt"],
    "isone": ["energy_da", "energy_rt_5"],
    "nyiso": ["energy_da", "energy_rt_5"],
    "spp": ["energy_da", "energy_rt"],
    "pjm": ["energy_da", "energy_rt_5"],
    "miso": ["energy_da", "energy_rt"],
}
output_energy_products = ["energy_da", "energy_rt"]
PRODUCT_END_DATE_OFFSET = {
        'energy_rt_5': pd.DateOffset(minutes=5),
        'energy_rt_15': pd.DateOffset(minutes=15),
        'energy_da': pd.DateOffset(hours=1)
}

START_DATE_NAME = 'STARTDATE'
END_DATE_NAME = 'ENDDATE'
PRODUCT_NAME = 'PRODUCT' 
PRICE_NAME = 'PRICE'
ISO_NAME = 'ISO'
NODE_NAME = 'NODE'
UPDATE_DATETIME_NAME = 'UPDATEDATETIME'
TIMESTAMP_NAME = 'TIMESTAMP'
POWERSIMM_DATE_FORMAT = '%d%b%Y:%H:%M:%S'


def parse_args():
    """
    Utility method for parsing command line arguments.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--powersimm-query",
        required=True,
    )
    parser.add_argument(
        "--start-date",
        required=False,
    )
    parser.add_argument(
        "--end-date",
        required=False,
    )
    parser.add_argument(
        "--output-file",
        required=True,
    )
    parser.add_argument(
        "--log-file",
        required=True,
    )
    args = parser.parse_args()
    return args

def ping_test():
    '''
    Calls the ping endpoint to see if Traverse is operational. If signal is
    not received after the maximum number of attempts it will error out.

    '''
    utc = pytz.utc
    attempt = 0
    max_attempts = 5
    success = False

    while not success:
        logging.info(f'Attempting ping test, attempt {attempt} of {max_attempts}')
        attempt += 1
        auth_context = adal.AuthenticationContext(AUTHORITY_URL, api_version=None)
        token = auth_context.acquire_token_with_client_credentials(
            RESOURCE, CLIENT_ID, CLIENT_SECRET
        )
        access_token = token["accessToken"]
        headers = {"Authorization": f"Bearer {access_token}"}
        try:
            output = requests.post(url=API_PING_ENDPOINT, headers=headers).content
            success = True
        except Exception as e:
            logging.info(e)
            if attempt == max_attempts:
                raise Exception("Unable to reach API. Please try again later")

def get_response_json(iso,node,product,start_date,end_date):
    logging.info(f'Harvesting data for {start_date} to {end_date}')
    logging.info(f'Harvesting data for product: {product}, iso: {iso.upper().replace("ISO-NE", "ISONE")}, node: {node.upper()}')
    response = {}
    data = {}
    product_entry = {}
    product_entry["dataType"] = "AscendISO"
    product_entry["params"] = {
        "iso": iso.upper().replace("ISO-NE", "ISONE"),
        "node": node.upper(),
        "product": product,
        "startDate": start_date.strftime('%m/%d/%Y'),
        "endDate": end_date.strftime('%m/%d/%Y'),
        "useCache": 'false',
    }
    data[product] = product_entry

    attempt = 0
    max_attempts = 5
    success = False
    while not success:
        attempt += 1
        try:
            auth_context = adal.AuthenticationContext(
                AUTHORITY_URL, api_version=None
            )
            token = auth_context.acquire_token_with_client_credentials(
                RESOURCE, CLIENT_ID, CLIENT_SECRET
            )
            access_token = token["accessToken"]
            headers = {"Authorization": f"Bearer {access_token}"}
            output = requests.post(
                url=API_ENDPOINT, json=data, headers=headers
            ).content
            response.update(json.loads(output))
            success = True
        except Exception as e:
            logging.info(e)
            if attempt == max_attempts:
                if "output" in locals():
                    raise Exception(
                        f"Max attempts reached, API call unsuccessful.\nFirst 100 characters of API output: {output[:100]}"
                    )
                else:
                    raise Exception(
                        "Max attempts reached, API call unsuccessful."
                    )
            time.sleep(
                5 * 2 ** (attempt - 1) + np.random.uniform() - 0.5
            )  # should sleep 5, 10, 20, 40 (all +- 0.5s to stagger retries)
    
    if "message" in response.keys():
        raise Exception("API call unsuccessful: " + response["message"])
    return response

def get_stream_data_as_long_df(iso, node, product, start_date, end_date):
    '''
    Makes a call to the get-data endpoint returns the results. Given node, iso and product it will return
    the market values.

    '''
    ping_test()
    
    # divide up the date ranges to make the calls less taxing
    num_years_in_range = end_date.year - start_date.year
    date_range = pd.date_range(start=start_date,end=end_date,periods=2+num_years_in_range)
    
    output_df_list = []
    for left_date, right_date in zip(date_range,date_range[1:]):
        response = get_response_json(iso,node,product,start_date,end_date)
        columns = response[product]["columns"]
        data = response[product]["data"]

        timestamps = pd.to_datetime([val[0] for val in data])
        prices = [val[1] for val in data]
        product_data = {TIMESTAMP_NAME:timestamps,PRICE_NAME:prices, PRODUCT_NAME:product, ISO_NAME:iso.lower(), NODE_NAME:node.lower()}
        product_df = pd.DataFrame(data=product_data)
        output_df_list.append(product_df)
    df = pd.concat(output_df_list)
    return df 

def update_df_to_powersimm_format(df):
    # assume traverse always returns timestamp beggining
    df[START_DATE_NAME] = df[TIMESTAMP_NAME] 
    df[END_DATE_NAME]   = df[[TIMESTAMP_NAME,PRODUCT_NAME]].apply(lambda row: row[TIMESTAMP_NAME]+PRODUCT_END_DATE_OFFSET[row[PRODUCT_NAME]],axis=1)
    df[START_DATE_NAME] = df[START_DATE_NAME].dt.strftime(POWERSIMM_DATE_FORMAT)
    df[END_DATE_NAME]   = df[END_DATE_NAME].dt.strftime(POWERSIMM_DATE_FORMAT)
    df[UPDATE_DATETIME_NAME] = pd.Timestamp.now().strftime(POWERSIMM_DATE_FORMAT)
    df = df[[START_DATE_NAME,END_DATE_NAME,PRICE_NAME,UPDATE_DATETIME_NAME]]

    return df 

if __name__ == "__main__":

    try:
        args = parse_args()
        try:
            logging.basicConfig(level=logging.DEBUG,filename=args.log_file,  format='%(levelname)s - %(message)s')
        except:
            logging.basicConfig(level=logging.DEBUG,filename=args.log_file, filemode='w', format='%(levelname)s - %(message)s')
        logging.debug('\n\nCALLING TRAVERSE\n\n')

        start_date = pd.to_datetime(args.start_date,format=POWERSIMM_DATE_FORMAT)
        end_date = pd.to_datetime(args.end_date,format=POWERSIMM_DATE_FORMAT)
        powersimm_query = args.powersimm_query.lower()
        iso_node_product_dict = {x.split('=')[0]:x.split('=')[1] for x in powersimm_query.split(',')}

        try:
            iso     = iso_node_product_dict['iso'] 
            node    = iso_node_product_dict['node'] 
            product = iso_node_product_dict['product'] 
        except IndexError as e:
            logging.info(e)
            logging.info(f"Need query='iso=,node=,product=' got query={args.powersimm_query}")
        
        df = get_stream_data_as_long_df(iso,node,product,start_date,end_date)
        df = update_df_to_powersimm_format(df)
        df.dropna(inplace=True)
        df.to_csv(args.output_file,index=False)
        

    except Exception:  # pylint: disable=broad-except
        logging.exception("Fatal error in getting traverse data entry point")
        try:
            empty_df = pd.DataFrame(columns=[START_DATE_NAME,END_DATE_NAME,PRICE_NAME,UPDATE_DATETIME_NAME])
            empty_df.to_csv(args.output_file,index=False)
        except:
            logging.exception("Cannot produce empty df please specify output-file")
# python .\traverse_request.py --output-file 'out.csv' --log-file 'log'--start-date '01Jan2018:00:00:00' --end-date '02Jan2018:00:00:00' --powersimm-query 'iso=CAISO,node=0096WD_7_N001,product=energy_rt_5' 

