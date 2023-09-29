import pandas as pd
import numpy as np
from scipy.interpolate import CubicSpline
from utils import timeit, read_json, get_number_of_timeframes_in_one_day

from tqdm import tqdm
import os
import time
from datetime import datetime, date, timedelta
import logging 
from telegram_log_handler import TelegramLogHandler

from influxdb_client import InfluxDBClient
from influxdb_wrapper import InfluxDBWrapper

import json

logging.basicConfig(
    filename='log/market_data_builder.log',
    encoding='utf-8', 
    level=logging.INFO,
    format='%(asctime)s %(levelname)-8s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S')

config = read_json('config.json')
logger = logging.getLogger(__name__)
if config['telegram']['enabled'] == True:
    telegram_handler = TelegramLogHandler(config['telegram']['bot_token'], config['telegram']['chat_id'])
    logger.addHandler(telegram_handler)

class MarketDataBuilder():
    
    def __init__(self, config_path) -> None:
        """
        Parameters:
        - path (str): Path to the config file.
        - currency (str): Currency symbol.

        """
        self.db_client = None
        self.config = read_json(config_path)

    def get_smile_for_expiry(self, data):
        """
        Retrieves the smile (volatility values) for a given expiry from the input data.
        
        Parameters:
        - data (numpy.ndarray): Option data containing columns for call/put flag, delta, bid_iv, mid_iv, and ask_iv.
        
        Returns:
        - numpy.ndarray: Smile data consisting of delta, bid_iv, mid_iv, and ask_iv columns.
        """
        cp = data[:,0]
        delta = data[:,1]
        bid_iv = data[:,2]
        mid_iv = data[:,3]
        ask_iv = data[:,4]

        delta_range = [0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95]

        deltas = []
        bid_ivs = []
        mid_ivs = []
        ask_ivs = []
        for i in range(len(cp)):

            # Adjust the delta based on the option type
            if cp[i] == 'P':
                delta[i] *= -1
            elif cp[i] == 'C':
                delta[i] = -delta[i] + 1

            deltas.append(delta[i])
            bid_ivs.append(bid_iv[i])
            mid_ivs.append(mid_iv[i])
            ask_ivs.append(ask_iv[i])
        
        # Define the mapping between delta categories and numerical values
        delta_mapping = {
            0.05: '5P',
            0.1 : '10P',
            0.15: '15P',
            0.2 : '20P',
            0.25: '25P',
            0.3 : '30P',
            0.35: '35P',
            0.4 : '40P',
            0.45: '45P',
            0.5 : 'ATM',
            0.55: '45C',
            0.6 : '40C',
            0.65: '35C',
            0.7 : '30C',
            0.75: '25C',
            0.8 : '20C',
            0.85: '15C',
            0.9 : '10C',
            0.95: '5C'}

        # necessary steps to order by increasing deltas (required for CubicSpline interpolation)
        vols = np.array([deltas, bid_ivs, mid_ivs, ask_ivs]).transpose()
        vols = vols[vols[:,0].argsort()]

        bid_iv_cs = CubicSpline(vols[:,0], vols[:,1])
        mid_iv_cs = CubicSpline(vols[:,0], vols[:,2])
        ask_iv_cs = CubicSpline(vols[:,0], vols[:,3])
    

        spline_bid_iv = bid_iv_cs(delta_range)
        spline_mid_iv = mid_iv_cs(delta_range)
        spline_ask_iv = ask_iv_cs(delta_range)

        smile = np.column_stack((list(delta_mapping.values()),
                                spline_bid_iv,
                                spline_mid_iv,
                                spline_ask_iv))

        return smile

    def process_option_data_file(self, option_data):
        """
        Processes the option data file by recreating the 5m timestamp, unpack the greeks data and format the expiry.

        Parameters:
        - option_data (pandas.DataFrame): Option data dataframe.

        Returns:
        - pandas.DataFrame: Processed option data dataframe.
        """
        option_data['timestamp_datetime'] = pd.to_datetime(option_data['timestamp'], unit='ms')
        option_data['timestamp_datetime'] = option_data['timestamp_datetime'].dt.tz_localize('UTC')
        option_data['timestamp_datetime'] = option_data['timestamp_datetime'] - pd.to_timedelta(option_data['timestamp_datetime'].dt.minute % 5, unit='m')
        option_data['timestamp_datetime'] = option_data['timestamp_datetime'] - pd.to_timedelta(option_data['timestamp_datetime'].dt.second, unit='s')
        option_data['timestamp_datetime'] = option_data['timestamp_datetime'] - pd.to_timedelta(option_data['timestamp_datetime'].dt.microsecond, unit='us')

        option_data = option_data.reset_index(drop=True)
        option_data[['delta', 'gamma', 'rho', 'theta', 'vega']] = pd.DataFrame(option_data['greeks'].to_list())
        option_data[['asset', 'expiry', 'strike', 'cp']] = pd.DataFrame(option_data['instrument_name'].str.split('-', expand=True))
        option_data['expiry'] = pd.to_datetime(option_data['expiry'], format='%d%b%y')
        
        return option_data

    def get_option_data(self, path):
        """
        Reads and retrieves the option data from a given file path.
        
        Parameters:
        - path (str): Path to the option data file.
        
        Returns:
        - pandas.DataFrame: Option data dataframe.
        """
        option_data = pd.read_parquet(path)
        option_data = self.process_option_data_file(option_data)
        
        return option_data

    def get_vol_surface_on_time(self, data, obs_time):
        """
        Retrieves the volatility surface and forward curves for a specific observation time from the input data.
        
        Parameters:
        - data (pandas.DataFrame): Option data dataframe.
        - obs_time (pandas.Timestamp): Observation time for the volatility surface.
        
        Returns:
        - dict: Dictionary containing the volatility surface data.
        - dict: Dictionary containing the forward curves data.
        """
        data = data[data['timestamp_datetime'] == obs_time]
        surface = {}
        forwards = {}
        for expiry in data['expiry'].unique():
            try:
                # filter out smiles for today's expiries if time is greater than 7:30 UTC (less than 30mn before expiry)
                raw_vol = data[(data['expiry'] + pd.to_timedelta('7:30:00') > obs_time.tz_localize(None)) & (data['expiry'] == expiry)][['cp', 'delta', 'bid_iv', 'mark_iv', 'ask_iv']].drop_duplicates(subset='delta')
                
                # keep only OTM options and with delta higher than 5%
                raw_vol = raw_vol[(raw_vol['delta'] < 0.5) & (raw_vol['delta'] > -0.5) & (raw_vol['delta'].abs() > 0.05)]
                
                #skip if less than 5 quotes or if only calls or puts quotes
                if raw_vol.shape[0] < 5 or raw_vol['cp'].value_counts().shape[0] < 2:
                    continue
                smile_data = self.get_smile_for_expiry(raw_vol.to_numpy())
            
                smile = pd.DataFrame({'delta':smile_data[:,0].astype(str),
                                    'bid_iv':smile_data[:,1].astype(float),
                                    'mid_iv':smile_data[:,2].astype(float),
                                    'ask_iv':smile_data[:,3].astype(float)})
                surface[pd.to_datetime(expiry).date()] = smile
                forwards[pd.to_datetime(expiry).date()] = data[data['expiry'] == expiry]['underlying_price'].mean()
            except Exception as e:
                logger.error(f"get_vol_surface_on_time failed with {e} on obs_time = {obs_time} and expiry = {expiry}")
            
        return surface, forwards

    def extract_vol_surfaces_from_file(self, datafile):
        """
        Extracts the volatility surfaces and forward curves from a given data file.
        
        Parameters:
        - datafile (str): Path to the option data file.
        
        Returns:
        - dict: Dictionary containing the volatility surfaces data.
        - dict: Dictionary containing the forward curves data.
        """
        option_data = self.get_option_data(datafile)
        surfaces = {}
        forward_curves = {}
        timestamps = option_data['timestamp_datetime'].unique()
        for obs_time in timestamps:
            surfaces[obs_time], forward_curves[obs_time] = self.get_vol_surface_on_time(option_data, obs_time)

        return surfaces, forward_curves

    def save_surfaces_to_db(self, vol_surfaces, forward_curves, conn):
        """
        Saves the volatility surfaces and forward curves to a SQLite database.
        
        Parameters:
        - vol_surfaces (dict): Dictionary containing the volatility surfaces data.
        - forward_curves (dict): Dictionary containing the forward curves data.
        - conn (sqlite3.Connection): SQLite database connection object.
        
        Returns:
        - bool: True if the saving process is successful, False otherwise.
        """
        success = False
        try:
            # Create a table to store the snapshots if it doesnt exist yet
            conn.execute('''CREATE TABLE IF NOT EXISTS snapshots (
                                snapshot_id TEXT PRIMARY KEY,
                                observation_time TEXT,
                                expiry TEXT,
                                smile_data TEXT,
                                underlying_price FLOAT
                            )''')
            
            cursor = conn.cursor()
            insert_values = []
            
            # Loop through the snapshots and surfaces to insert them into the database
            for observation_time, snapshot_surface in vol_surfaces.items():
                for expiry, smile_df in snapshot_surface.items():
                    forward = forward_curves[np.datetime64(observation_time)][expiry]
                    smile_data = smile_df.to_dict(orient='records')
                    smile_data_json = json.dumps(smile_data)


                    observation_time = str(observation_time)[0:19]
                    expiry = str(expiry)[0:10]
                    snapshot_id = f"{observation_time}_{expiry}"

                    # Check if the snapshot_id already exists in the database
                    cursor = conn.execute('SELECT COUNT(*) FROM snapshots WHERE snapshot_id = ?', (snapshot_id,))
                    result = cursor.fetchone()
                    count = result[0]

                    if count > 0:
                        # Update the existing row
                        conn.execute('UPDATE snapshots SET observation_time = ?, expiry = ?, smile_data = ?, underlying_price = ? WHERE snapshot_id = ?',
                                    (observation_time, expiry, smile_data_json, forward, snapshot_id))
                        logger.error(f"Snapshopt ID {snapshot_id} was replaced with the new data")
                    else:

                        insert_values.append((snapshot_id, observation_time, expiry, smile_data_json, forward))
            
            # Perform bulk insert of multiple rows
            if insert_values:
                cursor.executemany('INSERT INTO snapshots (snapshot_id, observation_time, expiry, smile_data, underlying_price) VALUES (?, ?, ?, ?, ?)',
                                insert_values)
            
            # # Commit the changes and close the connection
            conn.commit()
            conn.close()

            success = True

        except Exception as e:
            logger.error(f"An error occurred: {str(e)}")
            
        return success
    @timeit
    def save_future_order_book_to_influxdb(self, bucket, file_pattern, light_book = True, move_old_files=False):
        """
        Saves the future order book data to an InfluxDB database.
        
        Parameters:
        - bucket (str): InfluxDB bucket name.
        - pattern (str): File pattern for selecting the order book data files.
        - light_book(bool) : Save a light version of the order book (e.g. no depth of order book)
        - move_old_files(bool) : Move the files older than today to the Processed folder once processed if True.
        
        Returns:
        - bool: True if the saving process is successful, False otherwise.
        """
        success = False
        files = [file.name for file in os.scandir(self.config['data_folder']) if not file.is_dir() and file_pattern in file.name and 'options' not in file.name]
        
        try:
            with InfluxDBClient(url=self.config['database']['url'], token=self.config['database']['token'], org=self.config['database']['org'], timeout=30_000) as client:
                for file in tqdm(files, total=len(files)):
                    data = pd.read_parquet(f"{self.config['data_folder']}/{file}")

                    if light_book == True:
                        columns_to_keep = ['timestamp', 'settlement_price', 'open_interest', 'mark_price', 'last_price', 'instrument_name',\
                                            'index_price', 'best_bid_price', 'best_bid_amount', 'best_ask_price', 'best_ask_amount']
                        data = data[columns_to_keep]


                    data['timestamp'] = pd.to_datetime(data['timestamp'], unit='ms')
                    with client.write_api() as write_api:
                        write_api.write(bucket=bucket, 
                                        record=data, 
                                        data_frame_measurement_name="future_order_book",
                                        data_frame_tag_columns=['instrument_name'],
                                        data_frame_timestamp_column='timestamp')
                                        # time_precision='s')

                    processed_folder = f"{self.config['data_folder']}/Processed"
                    if not os.path.exists(processed_folder):
                        os.makedirs(processed_folder)
                        
                    if move_old_files == True:
                        yesterday = date.today() - timedelta(days=1)
                        file_date = datetime.strptime(file.split("_")[2], "%Y%m%d").date()
                        if file_date <= yesterday:
                            os.rename(f"{self.config['data_folder']}/{file}", f"{self.config['data_folder']}/Processed/{file}")

                    success = True
        except Exception as e:
                    logger.error(f"An error occurred with save_order_book_to_influxdb: {str(e)}")
    @timeit
    def save_order_book_to_influxdb(self, bucket, file_pattern, light_book = True, move_old_files=False):
        """
        Saves the order book data to an InfluxDB database.
        
        Parameters:
        - bucket (str): InfluxDB bucket name.
        - pattern (str): File pattern for selecting the order book data files.
        - light_book(bool) : Save a light version of the order book (e.g. no depth of order book)
        - move_old_files(bool) : Move the files older than today to the Processed folder once processed if True.
        
        Returns:
        - bool: True if the saving process is successful, False otherwise.
        """
        success = False
        files = [file.name for file in os.scandir(self.config['data_folder']) if not file.is_dir() and file_pattern in file.name and 'futures' not in file.name]
        
        try:
            with InfluxDBClient(url=self.config['database']['url'], token=self.config['database']['token'], org=self.config['database']['org'], timeout=30_000) as client:
                for file in tqdm(files, total=len(files)):
                    data = self.get_option_data(f"{self.config['data_folder']}/{file}")

                    if light_book == True:
                        data['strike'] = data['strike'].astype(int)
                        data['moneyness'] = (data['underlying_price'] - data['strike']) * np.where(data['cp'] == 'C', 1, -1)
                        columns_to_keep = ['underlying_price', 'timestamp', 'settlement_price', 'open_interest', 'mark_price', 'mark_iv', 'last_price', 'instrument_name',\
                                            'index_price', 'bid_iv', 'best_bid_price', 'best_bid_amount', 'best_ask_price', 'best_ask_amount', 'ask_iv', 'delta',\
                                            'gamma', 'rho', 'theta', 'vega', 'asset', 'expiry', 'strike', 'cp']
                        data = data[columns_to_keep]


                    data['timestamp'] = pd.to_datetime(data['timestamp'], unit='ms')
                    with client.write_api() as write_api:
                        write_api.write(bucket=bucket, 
                                        record=data, 
                                        data_frame_measurement_name="order_book",
                                        data_frame_tag_columns=['instrument_name'],
                                        data_frame_timestamp_column='timestamp')
                                        # time_precision='s')

                    processed_folder = f"{self.config['data_folder']}/Processed"
                    if not os.path.exists(processed_folder):
                        os.makedirs(processed_folder)
                        
                    if move_old_files == True:
                        yesterday = date.today() - timedelta(days=1)
                        file_date = datetime.strptime(file.split("_")[2], "%Y%m%d").date()
                        if file_date <= yesterday:
                            os.rename(f"{self.config['data_folder']}/{file}", f"{self.config['data_folder']}/Processed/{file}")

                    success = True
        except Exception as e:
                    logger.error(f"An error occurred with save_order_book_to_influxdb: {str(e)}")
    @timeit
    def save_surfaces_to_influxdb(self, bucket, file_pattern, move_old_files=False):
        """
        Saves the volatility surfaces and forward curves to an InfluxDB database.
        
        Parameters:
        - bucket (str): InfluxDB bucket name.
        - file_pattern (str): File pattern for selecting the option data files.
        - move_old_files(bool) : Move the files older than today to the Processed folder once processed if True.
        
        Returns:
        - bool: True if the saving process is successful, False otherwise.
        """
        success = False

        try:
            with InfluxDBClient(url=self.config['database']['url'], token=self.config['database']['token'], org=self.config['database']['org'], timeout=30_000) as client:
            
                with client.write_api() as write_api:

                    files = [file.name for file in os.scandir(self.config['data_folder']) if not file.is_dir() and file_pattern in file.name and 'futures' not in file.name]
                    for file in tqdm(files, total=len(files)):
                        logger.info(f'Processing {file} in save_surfaces_to_influxdb')
                        vol_surfaces, forward_curves = self.extract_vol_surfaces_from_file(f"{self.config['data_folder']}/{file}")

                        
                        # Loop through the snapshots and surfaces to insert them into the database
                        for observation_time, snapshot_surface in vol_surfaces.items():
                            for expiry, smile_df in snapshot_surface.items():
                                forward = forward_curves[observation_time][expiry]
                                smile_df['obs_time'] = observation_time
                                smile_df['underlying_price'] = forward
                                smile_df['expiry'] = expiry

                                write_api.write(bucket=bucket, 
                                                record=smile_df, 
                                                data_frame_measurement_name="volatility",
                                                data_frame_tag_columns=['delta', 'expiry'],
                                                data_frame_timestamp_column='obs_time')
                        
                        if move_old_files == True:
                            yesterday = date.today() - timedelta(days=1)
                            file_date = datetime.strptime(file.split("_")[2], "%Y%m%d").date()
                            if file_date <= yesterday:
                                logger.info(f'Moving file {file} to Processed folder')
                                os.rename(f"{self.config['data_folder']}/{file}", f"{self.config['data_folder']}/Processed/{file}")

                        success = True
        except Exception as e:
            logger.error(f"An error occurred with save_surfaces_to_influxdb: {str(e)}")

        return success
    @timeit
    def save_rr_analytics_to_influxdb(self, bucket_source, bucket_target):
        """
        Saves risk reversal analytics data to an InfluxDB database.
        Risk reversals are computed from the delta-based vol surfaces saved in 'bucket_source'.

        Parameters:
        - self: The instance of the class.
        - bucket_source (str): The name of the source bucket for risk reversal analytics data.
        - bucket_target (str): The name of the target bucket to save the data to.

        Returns:
        - bool: A boolean value indicating the success or failure of the data saving process.
        """
        deltas_rr = [5,15,25]
        tenors = ['7D', '2W', '1M', '2M', '3M', '6M', '1Y']
        end = datetime.now()
        start = datetime.strftime(end - timedelta(days = 3), "%Y-%m-%dT%H:%M:%SZ")
        end = datetime.strftime(end, "%Y-%m-%dT%H:%M:%SZ")
        fields = ['mid_iv']
        timeframe = '5m'

        success = True
        wrapper = InfluxDBWrapper(self.config['database']['url'], self.config['database']['token'], self.config['database']['org'], db_timeout=30_000)

        rr_vols = wrapper.get_historical_risk_reversal_by_delta_and_tenor(bucket_source, 'volatility',
                                                                            start,
                                                                            end,
                                                                            deltas_rr, tenors, fields, timeframe=timeframe)

        moving_averages = pd.DataFrame()
        for delta in deltas_rr:
            for tenor in tenors:
                data = rr_vols[(rr_vols['delta'] == delta) & (rr_vols['tenor'] == tenor)]

                new_data = pd.DataFrame()
                new_data['timestamp'] = data['timestamp']
                new_data['delta'] = delta
                new_data['tenor'] = tenor
                new_data['mavg'] = data['value'].rolling(2*get_number_of_timeframes_in_one_day(timeframe)).mean()
                std = data['value'].rolling(2*get_number_of_timeframes_in_one_day(timeframe)).std()
                new_data['upper'] = new_data['mavg'] + 3 * std
                new_data['lower'] = new_data['mavg'] - 3 * std

                new_data = new_data.loc[new_data['upper'].first_valid_index():]
                
                moving_averages = pd.concat([moving_averages, new_data], axis = 0)

        try:
            with InfluxDBClient(url=self.config['database']['url'], token=self.config['database']['token'], org=self.config['database']['org'], timeout=30_000) as client:
                with client.write_api() as write_api:
                    write_api.write(bucket=bucket_target, 
                                    record=rr_vols, 
                                    data_frame_measurement_name="risk_reversal",
                                    data_frame_tag_columns=['field', 'delta', 'tenor'],
                                    data_frame_timestamp_column='timestamp')
                    
                    write_api.write(bucket=bucket_target, 
                                    record=moving_averages, 
                                    data_frame_measurement_name="risk_reversal",
                                    data_frame_tag_columns=['delta', 'tenor'],
                                    data_frame_timestamp_column='timestamp')

        except Exception as e:
            success = False
            logger.error(f"An error occurred with save_rr_analytics_to_influxdb: {str(e)}")

        return success
    @timeit
    def save_eth_vs_btc_analytics_to_influxdb(self, bucket_source1, bucket_source2, bucket_target):
        """
        Saves ETH vs. BTC vol analytics data to an InfluxDB database.

        Parameters:
        - self: The instance of the class.
        - bucket_source1 (str): The name of the first source bucket for the Ethereum data.
        - bucket_source2 (str): The name of the second source bucket for the Bitcoin data.
        - bucket_target (str): The name of the target bucket to save the data to.

        Returns:
        - bool: A boolean value indicating the success or failure of the data saving process.
        """
        deltas = ['5P', '10P', '15P', '20P', '25P', '30P', '35P', '40P', '45P', 'ATM', '45C', '40C', '35C', '30C', '25C', '20C', '15C', '10C', '5C']
        tenors = ['7D', '2W', '1M', '2M', '3M', '6M', '1Y']
        end = datetime.now()
        start = datetime.strftime(end - timedelta(days = 3), "%Y-%m-%dT%H:%M:%SZ")
        end = datetime.strftime(end, "%Y-%m-%dT%H:%M:%SZ")
        fields = ['mid_iv']
        timeframe = '5m'

        success = True
        wrapper = InfluxDBWrapper(self.config['database']['url'], self.config['database']['token'], self.config['database']['org'], db_timeout=30_000)

        for delta in deltas:
            for tenor in tenors:
                # print(f"delta: {delta}, tenor {tenor}")
                vol_diff, leg_eth, leg_btc = wrapper.get_historical_vol_diff_by_delta_and_tenor(bucket_ccy1=bucket_source1,
                                                                     bucket_ccy2=bucket_source2,
                                                                     measurement='volatility',
                                                                     range_start=start,
                                                                     range_end=end,
                                                                     delta=delta,
                                                                     field=fields,
                                                                     tenor=tenor,
                                                                     timeframe=timeframe,
                                                                     include_vol_by_leg=True)
                vol_diff['value'] = vol_diff['value'].ffill()
                
                vol_diff['mavg'] = vol_diff['value'].rolling(2*get_number_of_timeframes_in_one_day(timeframe)).mean()
                std = vol_diff['value'].rolling(2*get_number_of_timeframes_in_one_day(timeframe)).std()
                vol_diff['upper'] = vol_diff['mavg'] + 3 * std
                vol_diff['lower'] = vol_diff['mavg'] - 3 * std

                vol_diff = vol_diff.loc[vol_diff['upper'].first_valid_index():]

                try:
                    with InfluxDBClient(url=self.config['database']['url'], token=self.config['database']['token'], org=self.config['database']['org'], timeout=30_000) as client:
                        with client.write_api() as write_api:
                            write_api.write(bucket=bucket_target, 
                                            record=vol_diff, 
                                            data_frame_measurement_name="eth_vs_btc",
                                            data_frame_tag_columns=['field', 'delta', 'tenor'],
                                            data_frame_timestamp_column='timestamp')
                            
                            write_api.write(bucket='eth_vol_analytics', 
                                            record=leg_eth.reset_index(), 
                                            data_frame_measurement_name="volatility",
                                            data_frame_tag_columns=['field', 'delta', 'tenor'],
                                            data_frame_timestamp_column='timestamp')
                            write_api.write(bucket='btc_vol_analytics', 
                                            record=leg_btc.reset_index(), 
                                            data_frame_measurement_name="volatility",
                                            data_frame_tag_columns=['field', 'delta', 'tenor'],
                                            data_frame_timestamp_column='timestamp')
                          
                except Exception as e:
                    success = False
                    logger.error(f"An error occurred with save_eth_vs_btc_analytics_to_influxdb: {str(e)}")

        return success
   
    @timeit
    def save_all_to_influxdb(self):
        """
        Saves various data to InfluxDB, including order books, volatility surfaces, and risk reversal analytics.
        """
        self.save_order_book_to_influxdb('eth_deribit_order_book', 'eth_5m', light_book=True, move_old_files=False)
        self.save_surfaces_to_influxdb('eth_vol_surfaces', file_pattern='eth_5m', move_old_files=True)
        self.save_order_book_to_influxdb('btc_deribit_order_book', 'btc_5m', light_book=True, move_old_files=False)
        self.save_surfaces_to_influxdb('btc_vol_surfaces', file_pattern='btc_5m', move_old_files=True)
        self.save_rr_analytics_to_influxdb(bucket_source='btc_vol_surfaces',\
                                          bucket_target='btc_vol_analytics')
        self.save_rr_analytics_to_influxdb(bucket_source='eth_vol_surfaces',\
                                          bucket_target='eth_vol_analytics')
        self.save_eth_vs_btc_analytics_to_influxdb(bucket_source1='eth_vol_surfaces',
                                                  bucket_source2='btc_vol_surfaces',
                                                  bucket_target='eth_vol_analytics')
        self.save_future_order_book_to_influxdb('eth_deribit_order_book', 'eth_5m', light_book=True, move_old_files=True)
        self.save_future_order_book_to_influxdb('btc_deribit_order_book', 'btc_5m', light_book=True, move_old_files=True)

if __name__ == "__main__":
    logger.info('Starting market data builder')
    md_builder = MarketDataBuilder('config.json')

    while True:
        md_builder.save_all_to_influxdb()
        time.sleep(300)