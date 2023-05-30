import pandas as pd
import numpy as np
from scipy.interpolate import CubicSpline
from utils import timeit, read_json

from tqdm import tqdm
import os

from influxdb_client import InfluxDBClient

import json


class MarketDataBuilder():
    
    def __init__(self, path, currency) -> None:
        """
        Parameters:
        - path (str): Path to the data folder containing the option data files.
        - currency (str): Currency symbol.

        """
        self.path = path
        self.currency = currency
        self.db_client = None

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

    @timeit
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
                raw_vol = data[(data['expiry'] + pd.to_timedelta('7:30:00') > obs_time.tz_localize(None)) & (data['expiry'] == expiry)][['cp', 'delta', 'bid_iv', 'mark_iv', 'ask_iv']]
                
                # keep only OTM options and with delta higher than 5%
                raw_vol = raw_vol[(raw_vol['delta'] < 0.5) & (raw_vol['delta'] > -0.5) & (raw_vol['delta'].abs() > 0.05)]
                
                #skip if less than 5 quotes
                if raw_vol.shape[0] < 5:
                    continue
                smile_data = self.get_smile_for_expiry(raw_vol.to_numpy())
            
                smile = pd.DataFrame({'delta':smile_data[:,0].astype(str),
                                    'bid_iv':smile_data[:,1].astype(float),
                                    'mid_iv':smile_data[:,2].astype(float),
                                    'ask_iv':smile_data[:,3].astype(float)})
                surface[pd.to_datetime(expiry).date()] = smile
                forwards[pd.to_datetime(expiry).date()] = data[data['expiry'] == expiry]['underlying_price'].mean()
            except Exception as e:
                print(f"failed with {e} on obs_time = {obs_time} and expiry = {expiry}")
            
        return surface, forwards

    @timeit
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

    @timeit
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
                        print(f"Snapshopt ID {snapshot_id} was replaced with the new data")
                    else:

                        insert_values.append((snapshot_id, observation_time, expiry, smile_data_json, forward))
            
            # Perform bulk insert of multiple rows
            if insert_values:
                cursor.executemany('INSERT INTO snapshots (snapshot_id, observation_time, expiry, smile_data, underlying_price) VALUES (?, ?, ?, ?, ?)',
                                insert_values)
            
            # # Commit the changes and close the connection
            conn.commit()
            # conn.close()

            success = True

        except Exception as e:
            print(f"An error occurred: {str(e)}")
            
        return success
    

    @timeit
    def save_surfaces_to_influxdb(self, bucket, file_pattern):
        """
        Saves the volatility surfaces and forward curves to an InfluxDB database.
        
        Parameters:
        - bucket (str): InfluxDB bucket name.
        - file_pattern (str): File pattern for selecting the option data files.
        
        Returns:
        - bool: True if the saving process is successful, False otherwise.
        """
        success = False

        try:
            with InfluxDBClient(url=config['database']['url'], token=config['database']['token'], org=config['database']['org'], timeout=30_000) as client:
            
                with client.write_api() as write_api:

                    files = [file.name for file in os.scandir(self.path) if not file.is_dir() and file_pattern in file.name]
                    for file in tqdm(files, total=len(files)):
                        print(f"Processing {file}...")
                        vol_surfaces, forward_curves = self.extract_vol_surfaces_from_file(f"{self.path}/{file}")

                        
                        # Loop through the snapshots and surfaces to insert them into the database
                        for observation_time, snapshot_surface in vol_surfaces.items():
                            for expiry, smile_df in snapshot_surface.items():
                                forward = forward_curves[observation_time][expiry]
                                # smile_data = smile_df.to_dict(orient='records')
                                # smile_data_json = json.dumps(smile_data)
                                smile_df['obs_time'] = observation_time
                                smile_df['underlying_price'] = forward
                                smile_df['expiry'] = expiry


                                # observation_time = str(observation_time)[0:19]
                                # expiry = str(expiry)[0:10]

                                write_api.write(bucket=bucket, 
                                                record=smile_df, 
                                                data_frame_measurement_name="volatility",
                                                data_frame_tag_columns=['delta', 'expiry'],
                                                data_frame_timestamp_column='obs_time')
                                                # time_precision='ms')

                        os.rename(f"{self.path}/{file}", f"{self.path}/Processed/{file}")
                        success = True
        except Exception as e:
            print(f"An error occurred : {str(e)}")

        return success


    @timeit
    def save_order_book_to_influxdb(self, bucket, file_pattern):
        """
        Saves the order book data to an InfluxDB database.
        
        Parameters:
        - bucket (str): InfluxDB bucket name.
        - pattern (str): File pattern for selecting the order book data files.
        
        Returns:
        - bool: True if the saving process is successful, False otherwise.
        """
        success = False
        if file_pattern == None:
            file_pattern = self.currency
        files = [file.name for file in os.scandir(self.path) if not file.is_dir() and file_pattern in file.name]
        
        try:
            with InfluxDBClient(url=config['database']['url'], token=config['database']['token'], org=config['database']['org'], timeout=30_000) as client:
                for file in tqdm(files, total=len(files)):
                    print(f"Processing {file}...")
                    data = self.get_option_data(f"{self.path}/{file}")

                    with client.write_api() as write_api:
                        write_api.write(bucket=bucket, 
                                        record=data, 
                                        data_frame_measurement_name="5min_order_book_measurement",
                                        data_frame_tag_columns=['instrument_name'],
                                        data_frame_timestamp_column='timestamp_datetime')
                                        # time_precision='ms')

                    processed_folder = f"{self.path}/Processed"
                    if not os.path.exists(processed_folder):
                        os.makedirs(processed_folder)
                        
                    os.rename(f"{self.path}/{file}", f"{processed_folder}/{file}")

                    success = True
        except Exception as e:
                    print(f"An error occurred : {str(e)}")


if __name__ == "__main__":
    
    config = read_json('config.json')
    
    md_builder_btc = MarketDataBuilder(config['data_folder'], 'btc')
    md_builder_eth = MarketDataBuilder(config['data_folder'], 'eth')

    # save full order books in InfluxDB 
    # md_builder_btc.save_order_book_to_influxdb('btc_deribit_order_book', 'btc_5m')
    # md_builder_eth.save_order_book_to_influxdb('eth_deribit_order_book', 'eth_5m')

    # save volatility surfaces in InfluxDB
    md_builder_btc.save_surfaces_to_influxdb('btc_vol_surfaces', file_pattern='btc_5m')
    md_builder_eth.save_surfaces_to_influxdb('eth_vol_surfaces', file_pattern='eth_5m')