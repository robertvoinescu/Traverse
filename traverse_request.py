import json
import os
import sys
import time
import math
from datetime import datetime
import pytz
import argparse
import os

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
SUPPORTED_MODES = ['spotprice','energy']

_NODE_RENAMING_DICT = {
    "nyiso":{
        "west":"west - a",
        "genese":"genese - b",
        "centrl":"centrl - c",
        "north":"north - d",
        "mhk vl":"mhk vl - e",
        "capitl":"capitl - f",
        "millwd":"millwd - h",
        "hud vl":"hud vl - g",
        "dunwod":"dunwod - i",
        "n.y.c.":"n.y.c. - j",
        "longil":"longil - k",
        "nyc":"n.y.c. - j"
    }
}


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
PRODUCT_FREQ = {
        'energy_rt_5': '5m',
        'energy_rt_15': '15m',
        'energy_da': '1h'
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
SPOT_PRICE_ID_NAME = 'SPOTPRICEID'


def rename_node(node, iso):
    try:
        return _NODE_RENAMING_DICT[iso.lower()][node.lower()]
    except KeyError:
        return node

def parse_args():
    """
    Utility method for parsing command line arguments.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--start-date",
        required=False,
    )
    parser.add_argument(
        "--end-date",
        required=False,
    )
    parser.add_argument(
        "--output-folder",
        required=True,
    )
    parser.add_argument(
        "--log-file",
        required=True,
    )
    parser.add_argument(
        "--spotprice-id-table",
        required=True,
    )
    parser.add_argument(
        "--mode",
        required=False,
    )
    parser.add_argument(
        "--iso",
        required=True,
    )
    parser.add_argument(
        "--nodes",
        required=True,
    )
    parser.add_argument(
        "--product",
        required=False,
        default=''
    )
    parser.add_argument(
        "--cache",
        required=False,
        default='./spotpricedata.csv'
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

def update_df_to_spotprice_format(df):
    # assume traverse always returns timestamp beggining
    df[SPOT_PRICE_ID_NAME] = df.apply(lambda row: spot_map.iso_node_product_to_spot(row[ISO_NAME],row[NODE_NAME],row[PRODUCT_NAME]),axis=1)
    df[START_DATE_NAME] = df[TIMESTAMP_NAME] 
    df[END_DATE_NAME]   = df[[TIMESTAMP_NAME,PRODUCT_NAME]].apply(lambda row: row[TIMESTAMP_NAME]+PRODUCT_END_DATE_OFFSET[row[PRODUCT_NAME]],axis=1)
    df[START_DATE_NAME] = df[START_DATE_NAME].dt.strftime(POWERSIMM_DATE_FORMAT)
    df[END_DATE_NAME]   = df[END_DATE_NAME].dt.strftime(POWERSIMM_DATE_FORMAT)
    df[UPDATE_DATETIME_NAME] = pd.Timestamp.now().strftime(POWERSIMM_DATE_FORMAT)
    df = df[[SPOT_PRICE_ID_NAME,START_DATE_NAME,END_DATE_NAME,PRICE_NAME,UPDATE_DATETIME_NAME]]
    # sometimes we don't have a spot price id map which results in null data so
    # we drop all the rows containing null data
    df = df.dropna()
    return df 

def update_df_to_energy_format(df):
    # assume traverse always returns timestamp beggining
    df = df[[TIMESTAMP_NAME,PRODUCT_NAME,PRICE_NAME]]
    dfpivot = df.pivot(index = TIMESTAMP_NAME, columns= PRODUCT_NAME, values=PRICE_NAME).reset_index()
    dfpivot = dfpivot.rename(columns={TIMESTAMP_NAME:'timestamp'})
    return dfpivot 

def determine_dates_from_supplemental_data(df,iso,node,product,start_date,end_date):
    iso_mask = df[ISO_NAME] == iso
    node_mask = df[NODE_NAME] == node
    product_mask = df[ISO_NAME] == product
    df = df[(iso_mask) & (product_mask) & (node_mask)]
    if df.empty:
        return start_date, end_date
    cache_date_range = pd.to_datetime(df[TIMESTAMP_NAME])
    cache_start = min(cache_date_range)
    cache_end = max(cache_date_range)
    # missing in between data so just load everything in
    if len(pd.date_range(cache_start,cache_end, freq=PRODUCT_END_DATE_OFFSET[product])) != len(cache_date_range):
        return start_date, end_date
    else:
        return cache_end, end_date

def read_spot_supplemental_data(table_location,spot_map,start_date,end_date):
    try:
        df = pd.read_csv(table_location)
    except:
        df = pd.DataFrame(columns=[TIMESTAMP_NAME, PRICE_NAME, PRODUCT_NAME, ISO_NAME, NODE_NAME])
        df[TIMESTAMP_NAME] = pd.to_datetime(df[TIMESTAMP_NAME])
        return df
    df = df[['SPOTPRICEID', START_DATE_NAME, END_DATE_NAME, PRICE_NAME]]
    df[ISO_NAME] = df['SPOTPRICEID'].map(spot_map.get_map('SPOTPRICEID',ISO_NAME))
    df[NODE_NAME] = df['SPOTPRICEID'].map(spot_map.get_map('SPOTPRICEID',NODE_NAME))
    df[PRODUCT_NAME] = df['SPOTPRICEID'].map(spot_map.get_map('SPOTPRICEID',PRODUCT_NAME))
    df = df.drop(columns=['SPOTPRICEID',END_DATE_NAME])
    df = df.rename(columns={START_DATE_NAME:TIMESTAMP_NAME})
    df[TIMESTAMP_NAME] = pd.to_datetime(df[TIMESTAMP_NAME],format=POWERSIMM_DATE_FORMAT)
    time_mask = (end_date <= df[TIMESTAMP_NAME]) & (df[TIMESTAMP_NAME]  <= start_date) 
    df = df[time_mask] 
    df = df.drop_duplicates([TIMESTAMP_NAME,ISO_NAME,NODE_NAME,PRODUCT_NAME])
    return df
    

class SpotPriceIdMap:
    def __init__(self, table_location):
        self.table_location = table_location
        table = pd.read_csv(table_location)
        table['LOOKUPIDVALUES'] = table['LOOKUPIDVALUES'].astype(str)
        table['LOOKUPIDVALUES'] = table['LOOKUPIDVALUES'].str.lower()
        table = table[table['LOOKUPIDVALUES'].str.contains("iso")]
        table['dict'] = table['LOOKUPIDVALUES'].apply(lambda row: {x.split('=')[0]:x.split('=')[1] for x in row.split(',')})
        table['ISO']        = table['dict'].apply(lambda x: x['iso'] )
        table['NODE']       = table['dict'].apply(lambda x: x['node'] )
        table['PRODUCT']    = table['dict'].apply(lambda x: x['product'] )
        self.table = table[['SPOTPRICEID',ISO_NAME,NODE_NAME,PRODUCT_NAME]]
        self.smap = None
    # map spot price id to iso, node, product

    def get_map(self,key,value):
        return dict(zip(self.table[key],self.table[value]))

    def iso_node_product_to_spot(self,iso,node,product):
        if self.smap is None:
            df = self.table
            df['frozen_set'] = df.apply(lambda row: frozenset([row['ISO'],row['NODE'],row['PRODUCT']]),axis=1) 
            self.smap = dict(zip(self.table['frozen_set'],self.table['SPOTPRICEID']))
        return self.smap.get(frozenset([iso,node,product]),None)


def get_products(start_date,end_date,iso,node,products,cache):
        df_list = []
        cache_iso_mask = cache[ISO_NAME] == iso
        cache_node_mask = cache[NODE_NAME] == node
        for product in products:
            cache_product_mask = cache[ISO_NAME] == product
            local_cache = cache[(cache_iso_mask) & (cache_product_mask) & (cache_node_mask)]
            trav_start_date, trav_end_date = determine_dates_from_supplemental_data(local_cache,iso,node,product,start_date,end_date) 
            df = get_stream_data_as_long_df(iso,node,product,trav_start_date,trav_end_date)
            df = df.append(local_cache)
            df.dropna(inplace=True)
            df_list.append(df)
        df = pd.concat(df_list)
        df = df.drop_duplicates([TIMESTAMP_NAME,ISO_NAME,NODE_NAME,PRODUCT_NAME],keep='last')
        return df

def post_process_and_save(df, start_date, end_date, iso, node, products, mode, output_folder,spot_map):
    if mode == 'energy':
        out_df = update_df_to_energy_format(df)
        start_year = start_date.year
        end_year = (end_date - pd.Timedelta('1 day')).year
        if start_year == end_year:
            csv_file_name = f"{node}_{start_year}.csv"
        else:
            csv_file_name = f"{node}_{start_year}_{end_year}.csv"
        out_df.to_csv(os.path.join(output_folder, csv_file_name), index = False)
    spot_df = update_df_to_spotprice_format(df)
    csv_file_name = f"spot_data.csv"
    spot_df.to_csv(os.path.join(output_folder, csv_file_name), index = False)


if __name__ == "__main__":
    try:
        args = parse_args()
        try:
            logging.basicConfig(level=logging.DEBUG,filename=args.log_file,  format='%(levelname)s - %(message)s')
        except:
            logging.basicConfig(level=logging.DEBUG,filename=args.log_file, filenmode='w', format='%(levelname)s - %(message)s')

        mode = args.mode.lower()
        start_date = pd.to_datetime(args.start_date,format=POWERSIMM_DATE_FORMAT)
        end_date = pd.to_datetime(args.end_date,format=POWERSIMM_DATE_FORMAT)
        if mode == 'energy':
            start_year = start_date.year
            end_year = (end_date - pd.Timedelta('1 day')).year
            start_date = pd.to_datetime(f"1/1/{start_year}")
            end_date = pd.to_datetime(f"1/2/{max(end_year,start_year+1)}")
        output_folder = args.output_folder
        iso     = args.iso.lower()
        nodes    = args.nodes.lower().split(',')
        nodes = [rename_node(node,iso) for node in nodes]

        if not mode in SUPPORTED_MODES:
            logging.exception("mode not supported")
            raise ValueError

        if mode == 'spotprice':
            products = [args.product.lower()]
        else:
            products = iso_energy_products[iso]

        spot_map = SpotPriceIdMap(args.spotprice_id_table)
        cache = read_spot_supplemental_data(args.cache,spot_map,start_date,end_date)

        for node in nodes:
            logging.debug('\n\nCALLING TRAVERSE\n\n')
            df = get_products(start_date,end_date,iso,node,products,cache)
            post_process_and_save(df, start_date, end_date, iso, node, products, mode, output_folder,spot_map)

    except Exception:  # pylint: disable=broad-except
        logging.exception("Fatal error in getting traverse data entry point")
        try:
            empty_df = pd.DataFrame(columns=[START_DATE_NAME,END_DATE_NAME,PRICE_NAME,UPDATE_DATETIME_NAME])
            empty_df.to_csv(args.output_file,index=False)
        except:
            logging.exception("Cannot produce empty df please specify output-file")

# python .\traverse_request.py --output-file 'out.csv' --log-file 'log'--start-date '01Jan2018:00:00:00' --end-date '02Jan2018:00:00:00' --iso CAISO --node 0096WD_7_N001 --product energy_rt_5 --spotprice-id-table 
