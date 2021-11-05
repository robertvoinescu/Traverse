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
N_PRODUCT_CHUNKS = 1

def parse_args():
    """
    Utility method for parsing command line arguments.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--node",
        required=True,
    )
    parser.add_argument(
        "--iso",
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


def get_stream_data(iso, node, start_date, end_date):
    '''
    Makes a call to the get-data endpoint returns the results. Given node and iso it will return
    all energy products present for the specified date range.
    '''

    ping_test()
    logging.info(f'Harvesting data for {iso.upper().replace("ISO-NE", "ISONE")} node: {node.upper()}')

    products_use_list = iso_energy_products[iso.lower().replace("iso-ne", "isone")]

    # to keep the requests small calls are chunked out by product
    products_chunked = [
        products_use_list[
            N_PRODUCT_CHUNKS
            * i : min(len(products_use_list), N_PRODUCT_CHUNKS * (i + 1))
        ]
        for i in range(math.ceil(len(products_use_list) / N_PRODUCT_CHUNKS))
    ]

    # to keep the request calls small we also divide up the time range for every
    # year present
    num_years_in_range = end_date.year - start_date.year
    date_range = pd.date_range(start=start_date,end=end_date,periods=2+num_years_in_range)

    output_dfs_list = []
    for left_date, right_date in zip(date_range,date_range[1:]):
        logging.info(f'Harvesting data for {left_date} to {right_date}')
        final_df = pd.DataFrame()
        response = {}
        for products_list in products_chunked:
            data = {}
            for product in products_list:
                logging.info(f'Harvesting data for product: {product}')
                product_entry = {}
                product_entry["dataType"] = "AscendISO"
                product_entry["params"] = {
                    "iso": iso.upper().replace("ISO-NE", "ISONE"),
                    "node": node.upper(),
                    "product": product,
                    "startDate": left_date.strftime('%m/%d/%Y'),
                    "endDate": right_date.strftime('%m/%d/%Y'),
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

        for product in products_use_list:
            logging.info(f'Processing product: {product}')
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

        if iso.lower() == "caiso":
            final_df['energy_rt'] = np.where(final_df['energy_rt_15'] >= 100, final_df['energy_rt_15'], final_df['energy_rt_5'])

        if iso.lower().replace('iso-ne', 'isone') in ["pjm", "nyiso", "isone"]:
            final_df = final_df.rename(columns={'energy_rt_5': 'energy_rt'})

        if right_date.year == pd.datetime.now().year:
            final_df = final_df.reset_index()
            final_df = final_df.loc[final_df['timestamp'] < pd.datetime.now()].set_index('timestamp')
        output_dfs_list.append(final_df)

    return pd.concat(output_dfs_list)


def output_dataframe(file, dataframe):
    """
    Writes dataframe to file with missing products as empty
    """
    for product in output_energy_products:
        if product not in dataframe.columns:
            dataframe[product] = ""
    dataframe = dataframe.tz_localize(None)
    dataframe.to_csv(file)



if __name__ == "__main__":
    try:
        args = parse_args()
        try:
            logging.basicConfig(level=logging.DEBUG,filename=args.log_file,  format='%(levelname)s - %(message)s')
        except:
            logging.basicConfig(level=logging.DEBUG,filename=args.log_file, filemode='w', format='%(levelname)s - %(message)s')
        logging.debug('\n\nCALLING TRAVERSE\n\n')
        start_date = pd.to_datetime(args.start_date)
        end_date = pd.to_datetime(args.end_date)
        df = get_stream_data(args.iso,args.node,start_date,end_date)
        output_dataframe(args.output_file,df)
    except Exception:  # pylint: disable=broad-except
        logging.exception("Fatal error in getting traverse data entry point")
# python .\traverse_request.py --start-date '1/1/2020' --end-date '1/2/2020' --node '0096WD_7_N001' --iso 'CAISO' --output-file 'out.csv' --log-file 'log'