import json
import os
import sys
import time
import math
from datetime import datetime
import pytz


import adal
import numpy as np
import pandas as pd
import requests

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


def ping_test():
    utc = pytz.utc
    start_time = utc.localize(datetime.utcnow())

    attempt = 0
    max_attempts = 5
    success = False

    while not success:
        attempt += 1
        auth_context = adal.AuthenticationContext(AUTHORITY_URL, api_version=None)
        token = auth_context.acquire_token_with_client_credentials(
            RESOURCE, CLIENT_ID, CLIENT_SECRET
        )
        access_token = token["accessToken"]
        headers = {"Authorization": f"Bearer {access_token}"}
        try:
            output = requests.post(url=API_PING_ENDPOINT, headers=headers).content
            end_time = utc.localize(datetime.utcnow())
            api_time = datetime.strptime(output.decode("utf-8"), "%m/%d/%Y %H:%M:%S %z")

            ping = (end_time - start_time).total_seconds()
            success = True
        except Exception as e:
            if attempt == max_attempts:
                raise Exception("Unable to reach API. Please try again later")


def get_stream_data(
    node, iso, start_date, end_date, output_file, use_cache="false", require_energy=False
):

    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(start_date)

    products_use_list = iso_energy_products[iso.lower().replace("iso-ne", "isone")]
    products_chunked = [
        products_use_list[
            N_PRODUCT_CHUNKS
            * i : min(len(products_use_list), N_PRODUCT_CHUNKS * (i + 1))
        ]
        for i in range(math.ceil(len(products_use_list) / N_PRODUCT_CHUNKS))
    ]
    years = range(int(start_date.year), int(end_date.year) + 1)
    output_dfs_list = []

    ping_test()

    for year in years:
        final_df = pd.DataFrame()
        response = {}
        for products_list in products_chunked:
            data = {}
            for product in products_list:
                product_entry = {}
                product_entry["dataType"] = "AscendISO"
                product_entry["params"] = {
                    "iso": iso.upper().replace("ISO-NE", "ISONE"),
                    "node": node.upper(),
                    "product": product,
                    "startDate": "1/1/" + str(year),
                    "endDate": "1/1/" + str(year + 1),
                    "useCache": use_cache.lower(),
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
            final_df["energy_rt"] = np.where(
                final_df["energy_rt_15"] >= 100,
                final_df["energy_rt_15"],
                final_df["energy_rt_5"],
            )

        if iso.lower().replace("iso-ne", "isone") in ["pjm", "nyiso", "isone"]:
            final_df = final_df.rename(columns={"energy_rt_5": "energy_rt"})

        if (year == years[-1]) and (year == pd.datetime.now().year):
            final_df = final_df.reset_index()
            final_df = final_df.loc[
                final_df["timestamp"] < pd.datetime.now()
            ].set_index("timestamp")

        for column in final_df.columns:
            fraction_missing = final_df[column].isnull().mean()
            if fraction_missing > 0.01:
                print("fill")
        output_dfs_list.append(final_df)

    outputDataframe(output_file, pd.concat(output_dfs_list))


def outputDataframe(file, dataframe):
    """
    Writes dataframe to file with missing products as empty
    """
    for product in output_energy_products:
        if product not in dataframe.columns:
            dataframe[product] = ""
    dataframe = dataframe.tz_localize(None)
    dataframe.to_csv(file)


iso = "CAISO"
node = "0096WD_7_N001"
start_date = "1/1/2021"
end_date = "1/2/2021"
#product_entry = {}
#data={}
#product_entry["params"] = {
#    "iso": iso.upper().replace("ISO-NE", "ISONE"),
#    "node": node.upper(),
#    "product": 'energy_rt_5',
#    "startDate": start_date,
#    "endDate": end_date,
#    "useCache": 'false',
#}
#product_entry["dataType"] = "AscendISO"
#
#data['energy_rt_5'] = product_entry
#auth_context = adal.AuthenticationContext(AUTHORITY_URL, api_version=None)
#token = auth_context.acquire_token_with_client_credentials(
#    RESOURCE, CLIENT_ID, CLIENT_SECRET
#)
#access_token = token["accessToken"]
#headers = {"Authorization": f"Bearer {access_token}"}
#out = requests.post(url=API_ENDPOINT, json=data, headers=headers)
#print(out)

output_file = "temp.csv"
get_stream_data(node, iso, start_date, end_date, output_file, require_energy=False)
