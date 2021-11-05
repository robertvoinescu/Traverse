import requests
import pandas as pd
import numpy as np
import json
import adal
import time
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))
from BatteryVal.Codebase.ascend_tools.logger import get_logger
import math
from datetime import datetime
import pytz

AUTHORITY_HOST_URL = 'https://login.microsoftonline.com'

script_path = os.path.realpath(__file__)
script_folder = os.path.realpath(os.path.join(script_path, '..'))

with open(os.path.join(script_folder, "stream_credentials.json")) as f:
    creds = json.load(f)

API_ENDPOINT = creds['api_endpoint']
API_PING_ENDPOINT = creds['api_ping_endpoint']
CLIENT_SECRET = creds["secret"]
RESOURCE = creds["resource"]
CLIENT_ID = creds["client_id"]
TENANT = creds["tenant"]
AUTHORITY_URL = f'{AUTHORITY_HOST_URL}/{TENANT}'

as_products = ["regup_rt", "regup_da", "regdown_rt", "regdown_da", "spin_rt", "spin_da", "nonspin_rt", "nonspin_da", "reg_rt", "reg_da", "reg_mile_rt", "reg_mile_ratio"]
use_cache_default_as = 'true'
use_cache_default_energy = 'false'
n_product_chunks = 1

iso_products = {
        "val": {
            "caiso": ["energy_da", "regup_rt", "regup_da", "regdown_rt", "regdown_da", "spin_rt", "spin_da", "nonspin_rt", "nonspin_da", "energy_rt_5", "energy_rt_15"],
            "ercot":  ["energy_da", "regup_da", "regdown_da", "spin_da", "nonspin_da", "energy_rt"],
            "isone": ["energy_da", "energy_rt_5", "reg_rt", "spin_rt", "nonspin_rt"],
            "nyiso": ["energy_da", "reg_da", "spin_da", "nonspin_da", "energy_rt_5", "reg_rt", "spin_rt", "nonspin_rt"],
            "spp":  ["energy_da", "regup_da", "regdown_da", "spin_da", "nonspin_da", "energy_rt", "regup_rt", "regdown_rt", "spin_rt", "nonspin_rt"],
            "pjm": ["energy_da", "energy_rt_5", "reg_rt", "spin_rt", "nonspin_rt", "reg_mile_rt", "reg_mile_ratio"],
            "miso": ["energy_da", "reg_da", "spin_da", "nonspin_da", "energy_rt", "reg_rt", "spin_rt", "nonspin_rt"]
        },
        "energy": {
            "caiso": ["energy_da", "energy_rt_5", "energy_rt_15"],
            "ercot": ["energy_da", "energy_rt"],
            "isone": ["energy_da", "energy_rt_5"],
            "nyiso": ["energy_da", "energy_rt_5"],
            "spp":  ["energy_da", "energy_rt"],
            "pjm": ["energy_da", "energy_rt_5"],
            "miso": ["energy_da", "energy_rt"]
        },
        "energy_da": {
            "caiso": ["energy_da"],
            "ercot": ["energy_da"],
            "isone": ["energy_da"],
            "nyiso": ["energy_da"],
            "spp":  ["energy_da"],
            "pjm": ["energy_da"],
            "miso": ["energy_da"]
        }
    }
output_products = {
    "val": ["energy_da", "regup_da", "regdown_da", "spin_da", "nonspin_da", "regup_rt", "regdown_rt", "spin_rt", "nonspin_rt", "energy_rt", "reg_da", "reg_rt"],
    "energy": ["energy_da", "energy_rt"],
    "energy_da": ["energy_da"]
}

extra_products = {'PJM':{'reg_rt': ["reg_rt", "reg_mile_rt", "reg_mile_ratio"]}, 'NYISO':{'reg_buyback_rt': ["reg_rt"]}}


def ping_test():
    logger = get_logger()
    utc = pytz.utc
    start_time = utc.localize(datetime.utcnow())

    attempt = 0
    max_attempts = 5
    success = False

    while not success:
        attempt += 1
        logger.info(f'Attempting ping test, attempt {attempt} of {max_attempts}')
        auth_context = adal.AuthenticationContext(AUTHORITY_URL, api_version=None)
        token = auth_context.acquire_token_with_client_credentials(RESOURCE, CLIENT_ID, CLIENT_SECRET)
        access_token = token['accessToken']
        headers = {
            'Authorization': f'Bearer {access_token}'
        }
        try:
            output = requests.post(url=API_PING_ENDPOINT, headers=headers).content
            end_time = utc.localize(datetime.utcnow())
            api_time  = datetime.strptime(output.decode("utf-8"), '%m/%d/%Y %H:%M:%S %z')
            logger.info(f'API returned time: {api_time}')

            ping = (end_time - start_time).total_seconds()
            logger.info(f'API response time is {ping} seconds')
            success = True
        except Exception as e:
            logger.info(e)
            if attempt == max_attempts:
                raise Exception('Unable to reach API. Please try again later')
    return


def determine_cache_usage(products_list, use_cache):
    if use_cache == '':
        all_as = np.all([prod in as_products for prod in products_list])
        if all_as:
            use_cache_instance = use_cache_default_as
        else:
            use_cache_instance = use_cache_default_energy
    else:
        use_cache_instance = use_cache

    return use_cache_instance

def get_stream_data(study_config, output_file, mode="val", use_cache='', require_energy=False):

    logger = get_logger()
    logger.info(f'Harvesting data for {study_config["region"].upper().replace("ISO-NE", "ISONE")} node: {study_config["price_stream"]["price_node"].upper()}')
    products_full_list = iso_products[mode][study_config["region"].lower().replace('iso-ne', 'isone')]
    if 'product_to_markets' in study_config:
        products_use_list = []
        for prod in study_config['product_to_markets']:
            if 'comment' not in prod:
                if study_config['product_to_markets'][prod] != '':
                    for mkt in study_config['product_to_markets'][prod]:
                        if not (prod == 'energy' and mkt == 'rt'):
                            products_use_list.append(f'{prod}_{mkt}')
        products_use_list += iso_products['energy'][study_config['region'].lower().replace('iso-ne', 'isone')].copy()
        if study_config["region"].upper() in extra_products:
            for prod_mkt in products_use_list.copy():
                if prod_mkt in extra_products[study_config["region"].upper()]:
                    products_use_list += extra_products[study_config["region"].upper()][prod_mkt]
                    products_use_list.remove(prod_mkt)
        products_use_list = list(set(products_use_list))
    else:
        products_use_list = products_full_list

    products_chunked = [products_use_list[n_product_chunks*i: min(len(products_use_list), n_product_chunks*(i+1))]  for i in range(math.ceil(len(products_use_list)/n_product_chunks)) ]
    years = range(int(study_config["price_stream"]["start_date"]), int(study_config["price_stream"]["end_date"]) + 1)
    output_dfs_list = []

    ping_test()

    logger.info(f'Full list of products to pull: {products_use_list}')
    for year in years:
        final_df = pd.DataFrame()
        response = {}
        for products_list in products_chunked:
            use_cache_instance = determine_cache_usage(products_list, use_cache)
            data = {}
            logger.info(f"Harvesting product{'' if n_product_chunks == 1 else 's'}: {products_list}")
            for product in products_list:
                product_entry = {}
                product_entry["dataType"] = "AscendISO"
                product_entry["params"] = {
                    "iso": study_config["region"].upper().replace('ISO-NE', 'ISONE'),
                    "node": study_config["price_stream"]["price_node"].upper(),
                    "product": product,
                    "startDate": "1/1/" + str(year),
                    "endDate":  "1/1/" + str(year+1),
                    "useCache": use_cache_instance,
                }

                data[product] = product_entry

            attempt = 0
            max_attempts = 5
            success = False
            while not success:
                attempt += 1
                logger.info(f'Harvesting data for {year}, attempt {attempt} of {max_attempts}')
                try:
                    auth_context = adal.AuthenticationContext(AUTHORITY_URL, api_version=None)
                    token = auth_context.acquire_token_with_client_credentials(RESOURCE, CLIENT_ID, CLIENT_SECRET)
                    access_token = token['accessToken']
                    headers = {
                        'Authorization': f'Bearer {access_token}'
                    }
                    output = requests.post(url=API_ENDPOINT, json=data, headers=headers).content
                    response.update(json.loads(output))
                    success = True
                except Exception as e:
                    logger.info(e)
                    if attempt == max_attempts:
                        if 'output' in locals():
                            raise Exception(f'Max attempts reached, API call unsuccessful.\nFirst 100 characters of API output: {output[:100]}')
                        else:
                            raise Exception('Max attempts reached, API call unsuccessful.')
                    time.sleep(5 * 2**(attempt-1) + np.random.uniform() - 0.5)  # should sleep 5, 10, 20, 40 (all +- 0.5s to stagger retries)

            if 'message' in response.keys():
                raise Exception('API call unsuccessful: ' + response['message'])

        for product in products_use_list:
            logger.info(f'Processing product: {product}')
            columns = response[product]["columns"]
            data = response[product]["data"]
            values = []

            for val in data:
                values.append(val[1])

            new_set = [x[1] for x in data]
            if len(final_df.columns) == 0:
                timestamps = []
                for val in data:
                    timestamps.append(val[0])
                final_df["timestamp"] = timestamps
                final_df["timestamp"] = pd.to_datetime(final_df["timestamp"])
                final_df = final_df.set_index("timestamp")

            final_df[product] = new_set

        if study_config["region"].lower() == "caiso":
            if mode in ["energy", "val"]:
                final_df['energy_rt'] = np.where(final_df['energy_rt_15'] >= 100, final_df['energy_rt_15'], final_df['energy_rt_5'])
            if mode == "val":
                final_df = final_df.drop(columns={'energy_rt_15', 'energy_rt_5'})

        if (study_config["region"].lower() == "pjm") and (mode == "val") and ('reg_rt' in final_df.columns):
            final_df['reg_rt'] = 0.95 * (final_df['reg_rt'] + final_df['reg_mile_rt'] * final_df['reg_mile_ratio'])
            final_df = final_df.drop(columns={'reg_mile_rt', 'reg_mile_ratio'})

        if study_config["region"].lower().replace('iso-ne', 'isone') in ["pjm", "nyiso", "isone"]:
            final_df = final_df.rename(columns={'energy_rt_5': 'energy_rt'})

        if (year == years[-1]) and (year == pd.datetime.now().year):
            final_df = final_df.reset_index()
            final_df = final_df.loc[final_df['timestamp'] < pd.datetime.now()].set_index('timestamp')

        for column in final_df.columns:
            fraction_missing = final_df[column].isnull().mean()
            if fraction_missing > 0.01:
                logger.info(f'{column} column is missing {round(100*fraction_missing, 2)}% of data in {year}')
            if require_energy and (column == 'energy_rt') and fraction_missing == 1:
                raise Exception(f'No rt energy data retrieved for {year}, check node name and start/end years')
        output_dfs_list.append(final_df)

    outputDataframe(output_file, pd.concat(output_dfs_list), mode)


def outputDataframe(file, dataframe, mode="val"):
    for product in output_products[mode]:
        if product not in dataframe.columns:
            dataframe[product] = ''
    dataframe = dataframe.tz_localize(None)
    dataframe.to_csv(file)
    return

get_stream_data(study_config, output_file, mode="val", use_cache='', require_energy=False):
