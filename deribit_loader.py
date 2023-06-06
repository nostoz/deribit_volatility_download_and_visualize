import requests
import time
import pandas as pd
from datetime import datetime
import logging
from telegram_log_handler import TelegramLogHandler
import gc
import concurrent.futures
import os
import traceback
from utils import read_json 

config = read_json('config.json')

logging.basicConfig(
    filename='log/deribit_loader.log',
    encoding='utf-8', 
    level=logging.INFO,
    format='%(asctime)s %(levelname)-8s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S')

logger = logging.getLogger(__name__)

if config['telegram']['enabled'] == True:
    telegram_handler = TelegramLogHandler(config['telegram']['bot_token'], config['telegram']['chat_id'])
    logger.addHandler(telegram_handler)

# Deribit API v2 base URL
base_url = "https://www.deribit.com/api/v2/"

# folder where will be stored the downloaded market data
data_folder = 'data'

coins = ['BTC', 'ETH']
timeframe = '5m'

timeframe_minutes = {
    "5m": 5,
    "15m": 15,
    "30m": 30,
    "1h": 60,
    "4h": 240,
    "1d": 1440
}

def fetch_instrument_data(inst, depth = 10):
    """
    Fetches order book data for the specified instrument.

    Args:
        inst (str): Name of the instrument to fetch data for.

    Returns:
        dict: The order book data for the specified instrument.
    """
    endpoint = "public/get_order_book"
    params = {
        "instrument_name": inst,
        "depth": depth
    }

    retries = 5
    while retries > 0:
        try:
            response = requests.get(base_url + endpoint, params=params)
        except Exception as e:
            logger.error(f"Failed to fetch data for {inst}.\nException arised during request: {e}")
            return None
        result = response.json()
        if 'error' in result and result['error']['code'] == 10028:
            wait_time = int(response.headers.get('retry-after', '2'))
            logger.warning(f"Too many requests, waiting for {wait_time} seconds")
            time.sleep(wait_time)
            retries -= 1
        elif 'result' in result:
            return result['result']
    
    # if we get here, it means the request failed
    logger.error(f"Failed to fetch data for {inst}.\nUnexpected response: {result}")
    return None



def fetch_available_instruments(currency, kind):
    """
    Fetches the available option instruments for the specified currency.

    Args:
        currency (str): The currency to fetch option instruments for.

    Returns:
        dict: The available option instruments for the specified currency.
    """

    endpoint = "public/get_instruments"
    params = {
        "currency": currency,
        "depth": 1,
        "kind" : kind,
        "expired" : "false"
    }
    
    response = requests.get(base_url + endpoint, params=params)
    return response.json()


def save_to_parquet(df, filename):
    """
    Saves the specified Pandas DataFrame to a Parquet file.

    Args:
        df (pd.DataFrame): The DataFrame to save.
        filename (str): The name of the file to save the DataFrame to.
    """
    try:
        df.to_parquet(filename)
    except Exception as e:
        logger.error(f"Failed to save\n {df}\n with exception {e}")


def fetch_and_save_data(currencies):
    """
    Fetches and saves the futures/options data for given currencies to Parquet files.
    """
    logger.info(f"Starting job at {datetime.now()}")

    instruments_per_request = 40 

    for currency in currencies:
        option_instruments = [inst['instrument_name'] for inst in fetch_available_instruments(currency, 'option')['result']]
        option_instruments_lists = [option_instruments[i:i+instruments_per_request] for i in range(0, len(option_instruments), instruments_per_request)]
        future_instruments = [inst['instrument_name'] for inst in fetch_available_instruments(currency, 'future')['result']]
        future_instruments_lists = [future_instruments[i:i+instruments_per_request] for i in range(0, len(future_instruments), instruments_per_request)]

        options_data = pd.DataFrame()
        for inst_list in option_instruments_lists:
            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                result = list(executor.map(fetch_instrument_data, inst_list))
                # Filter out None values from the result list
                result = [r for r in result if r is not None]
                if len(result) > 0:
                    options_data = pd.concat([options_data, pd.DataFrame(result)], axis=0)
            time.sleep(1)

        futures_data = pd.DataFrame()
        for inst_list in future_instruments_lists:
            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                result = list(executor.map(fetch_instrument_data, inst_list))
                # Filter out None values from the result list
                result = [r for r in result if r is not None]
                if len(result) > 0:
                    futures_data = pd.concat([futures_data, pd.DataFrame(result)], axis=0)
            time.sleep(1)

        # get the UTC time at the start of the current day
        current_day = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y%m%d")

        # aggregate new data with existing data
        opt_agg_file_name = f"{data_folder}/{currency.lower()}_{timeframe}_{current_day}_options_data.parquet"
        if os.path.exists(opt_agg_file_name):
            options_data = pd.concat([pd.read_parquet(opt_agg_file_name),options_data], axis = 0)
        save_to_parquet(options_data, opt_agg_file_name)

        fut_agg_file_name = f"{data_folder}/{currency.lower()}_{timeframe}_{current_day}_futures_data.parquet"
        if os.path.exists(fut_agg_file_name):
            futures_data = pd.concat([pd.read_parquet(fut_agg_file_name),futures_data], axis = 0)
        save_to_parquet(futures_data, fut_agg_file_name)

        del options_data
        del futures_data
        gc.collect()

    logger.info(f"Job completed at {datetime.now()}")


def get_next_run_time(timeframe):
    """
    Get the time until the next multiple of `timeframe` minutes and 00 seconds
    """
    current_time = time.localtime() 
    next_run = (timeframe_minutes[timeframe] - current_time.tm_min % timeframe_minutes[timeframe]) * 60 \
                - current_time.tm_sec

    return next_run % (timeframe_minutes[timeframe] * 60)


if __name__ == '__main__':
    logger.info("Loader started")

    while True:
        try:
            # Pause until the next multiple of `timeframe` minutes and 00 seconds
            next_run = get_next_run_time(timeframe)
            logger.info(f"next run in {next_run} seconds")
            time.sleep(next_run)

            # Run the job
            fetch_and_save_data(coins)

        except Exception:
            # Catch any exception that occurs
            logger.error("An exception occurred:")
            logger.error(traceback.format_exc())

            # Restart the program after a delay
            logger.info("Restarting the loader...")
            time.sleep(60)
            continue