from influxdb_client import InfluxDBClient
import pandas as pd
from datetime import datetime, timedelta

from utils import read_json, build_option_expiries, add_tenor, timeit



class InfluxDBWrapper():
    def __init__(self, db_url, db_token, db_org, db_timeout) -> None:
        """
        Args:
            db_url (str): The URL of the InfluxDB.
            db_token (str): The authentication token for the InfluxDB.
            db_org (str): The organization name in the InfluxDB.
            db_timeout (int): The timeout duration for the InfluxDB connection.
        """
        self.url = db_url
        self.token = db_token
        self.org = db_org
        self.timeout = db_timeout

    def get_smile_for_obs_time(self, bucket, measurement, expiry, obs_time, field = 'mid_iv'):
        """
        Retrieves the smile data for a specific observation time.

        Args:
            bucket (str): The name of the InfluxDB bucket.
            measurement (str): The measurement name in the InfluxDB.
            expiry (str): The expiration date of the options.
            obs_time (str): The observation time.
            field (str): The field to retrieve from the data. Default is 'mid_iv'.

        Returns:
            pandas.DataFrame: The smile data for the specified observation time.
        """
        with InfluxDBClient(url=self.url, token=self.token, org=self.org, timeout=self.timeout) as client:
            query = f'from(bucket: "{bucket}")\
                        |> range(start: {obs_time}, stop: {obs_time.replace("0Z", "1Z")})\
                        |> filter(fn: (r) => r._measurement == "{measurement}") \
                        |> filter(fn: (r) => r.expiry == "{expiry}")\
                        |> filter(fn: (r) => r._time == {obs_time})\
                        |> filter(fn: (r) => r._field == "{field}")'
            
            result = client.query_api().query(query)

            return pd.DataFrame(data=result.to_values(columns=['delta', '_value']), columns=['delta', field])

    def get_forward_curve_for_obs_time(self, bucket, measurement, obs_time):
        """
        Retrieves the forward curve data for a specific observation time.

        Args:
            bucket (str): The name of the InfluxDB bucket.
            measurement (str): The measurement name in the InfluxDB.
            obs_time (str): The observation time.

        Returns:
            pandas.DataFrame: The forward curve data for the specified observation time.
        """
        with InfluxDBClient(url=self.url, token=self.token, org=self.org, timeout=self.timeout) as client:
            query = f'from(bucket: "{bucket}")\
                        |> range(start: {obs_time}, stop: {obs_time.replace("0Z", "1Z")})\
                        |> filter(fn: (r) => r._measurement == "{measurement}") \
                        |> filter(fn: (r) => r._time == {obs_time})\
                        |> filter(fn: (r) => r.delta == "ATM")\
                        |> filter(fn: (r) => r._field == "underlying_price")'

            result = client.query_api().query(query)

            return pd.DataFrame(data=result.to_values(columns=['expiry', '_value']), columns=['expiry', 'forward'])
        
    def get_vol_surface_for_obs_time(self, bucket, measurement, obs_time, field):
        """
        Retrieves the volatility surface data for a specific observation time.

        Args:
            bucket (str): The name of the InfluxDB bucket.
            measurement (str): The measurement name in the InfluxDB.
            obs_time (str): The observation time.
            field (str): The field to retrieve from the data.

        Returns:
            pandas.DataFrame: The volatility surface data for the specified observation time.
        """
        with InfluxDBClient(url=self.url, token=self.token, org=self.org, timeout=self.timeout) as client:
            query = f'from(bucket: "{bucket}")\
                        |> range(start: {obs_time}, stop: {obs_time.replace("0Z", "1Z")})\
                        |> filter(fn: (r) => r._measurement == "{measurement}") \
                        |> filter(fn: (r) => r._time == {obs_time})\
                        |> filter(fn: (r) => r._field == "{field}")'
            
            result = client.query_api().query(query)

            data = pd.DataFrame(data=result.to_values(columns=['expiry', 'delta', '_value']), columns=['expiry', 'delta', 'vol'])
            
            deltas = ['5P', '10P', '15P', '20P', '25P', '30P', '35P', '40P', '45P', 'ATM',\
                       '45C', '40C', '35C', '30C', '25C', '20C', '15C', '10C', '5C']

            vol_surface = data.pivot(index='expiry', columns='delta', values='vol')[deltas]

            return vol_surface
        
    def get_historical_vol_for_delta_and_expiry(self, bucket, measurement, range_start, range_end, delta, expiry, field):
        """
        Retrieves the historical volatility data for a specific delta and expiry range.

        Args:
            bucket (str): The name of the InfluxDB bucket.
            measurement (str): The measurement name in the InfluxDB.
            range_start (str): The start of the range for retrieving historical data.
            range_end (str): The end of the range for retrieving historical data.
            delta (str): The delta value for the options.
            expiry (str): The expiration date of the options.
            field (str): The field to retrieve from the data.

        Returns:
            pandas.DataFrame: The historical volatility data for the specified delta and expiry range.
        """
        with InfluxDBClient(url=self.url, token=self.token, org=self.org, timeout=self.timeout) as client:
            query = f'from(bucket: "{bucket}")\
                        |> range(start: {range_start}, stop: {range_end})\
                        |> filter(fn: (r) => r._measurement == "{measurement}") \
                        |> filter(fn: (r) => r.expiry == "{expiry}")\
                        |> filter(fn: (r) => r.delta == "{delta}")\
                        |> filter(fn: (r) => r._field == "{field}")'

            result = client.query_api().query(query)

            return pd.DataFrame(data=result.to_values(columns=['_time', '_value']), columns=['timestamp', field])


    def get_historical_vol(self, bucket, measurement, range_start, range_end, field):
        """
        Retrieves the historical volatility data for a specified range.

        Args:
            bucket (str): The name of the InfluxDB bucket.
            measurement (str): The measurement name in the InfluxDB.
            range_start (str): The start of the range for retrieving historical data.
            range_end (str): The end of the range for retrieving historical data.
            field (str): The field to retrieve from the data.

        Returns:
            pandas.DataFrame: The historical volatility data for the specified range.
        """
        with InfluxDBClient(url=self.url, token=self.token, org=self.org, timeout=self.timeout) as client:
            query = f'from(bucket: "{bucket}")\
                        |> range(start: {range_start}, stop: {range_end})\
                        |> filter(fn: (r) => r._measurement == "{measurement}") \
                        |> filter(fn: (r) => r._field == "{field}")'

            result = client.query_api().query(query)

            return pd.DataFrame(data=result.to_values(columns=['_time', 'expiry', 'delta', '_value']), columns=['timestamp', 'expiry', 'delta', field])
    
    def _get_historical_nearby_expiries_for_tenor(self, range_start, range_end, tenor):
        """
        Private method to retrieve historical nearby expiries for a given tenor. 
        Required to calculate the volatility by tenor.

        Args:
            range_start (str): The start of the range for retrieving historical data.
            range_end (str): The end of the range for retrieving historical data.
            tenor (str): The tenor value for the options.

        Returns:
            pandas.DataFrame: The historical nearby expiries for the given tenor.
        """
        range_start = datetime.strptime(range_start, "%Y-%m-%dT%H:%M:%SZ").date()
        range_end = datetime.strptime(range_end, "%Y-%m-%dT%H:%M:%SZ").date()
        expiries = build_option_expiries(range_start)
        tenor_days = int(tenor[:-1])
        tenor_unit = tenor[-1]

        result = []
        current_day = range_start
        while current_day <= range_end:
            day_expiries = build_option_expiries(current_day)
            current_tenor = add_tenor(current_day, tenor)

            expiry1 = day_expiries[0]
            expiry2 = day_expiries[1]
            idx = 1
            while expiry2 < current_tenor:
                expiry1 = day_expiries[idx]
                expiry2 = day_expiries[idx+1]
                idx += 1

            result.append([current_day, expiry1, expiry2])
             
            current_day += timedelta(days=1)

        return pd.DataFrame(data=result, columns=['date', 'expiry1', 'expiry2']).set_index('date')
        
    @timeit
    def get_historical_vol_for_tenor(self, bucket, measurement, range_start, range_end, delta, tenor, field):
        """
        Retrieves the historical volatility data for a specific tenor.

        Args:
            bucket (str): The name of the InfluxDB bucket.
            measurement (str): The measurement name in the InfluxDB.
            range_start (str): The start of the range for retrieving historical data.
            range_end (str): The end of the range for retrieving historical data.
            delta (str): The delta value for the options.
            tenor (str): The tenor value for the options.
            field (str): The field to retrieve from the data.

        Returns:
            pandas.DataFrame: The historical volatility data for the specified tenor.
        """
        nearby_expiries = self._get_historical_nearby_expiries_for_tenor(range_start, range_end, tenor)
        
        nearby_expiries.index = pd.to_datetime(nearby_expiries.index) 
        
        unique_expiries = pd.concat([nearby_expiries['expiry1'], nearby_expiries['expiry2']], axis=0).unique()

        with InfluxDBClient(url=self.url, token=self.token, org=self.org, timeout=self.timeout) as client:
            result_df = pd.DataFrame(columns=['timestamp', 'expiry', 'delta', field])
            for expiry in unique_expiries:
                res = self.get_historical_vol_for_delta_and_expiry(bucket=bucket,
                                                                measurement='volatility',
                                                                range_start=range_start,
                                                                range_end=range_end,
                                                                delta=delta,
                                                                expiry=expiry,
                                                                field=field)
                res['delta'] = delta
                res['expiry'] = expiry
                result_df = pd.concat([result_df, res], axis=0)
            
            vols = result_df.pivot(index='timestamp', columns='expiry', values=field).tz_localize(None).ffill()

            new_index = pd.date_range(start=vols.index[0], end=vols.index[-1], freq='5T')
            nearby_expiries = nearby_expiries.reindex(new_index, method='ffill') 
            
            tenor_vols = pd.DataFrame(index=vols.index)

            tenor_vols['expiry1'] = pd.to_datetime(nearby_expiries['expiry1'])
            tenor_vols['expiry2'] = pd.to_datetime(nearby_expiries['expiry2'])

            tenor_vols['diff_to_exp1'] = abs((tenor_vols['expiry1'] - tenor_vols.index).dt.days)
            tenor_vols['diff_to_exp2'] = abs((tenor_vols['expiry2'] - tenor_vols.index).dt.days)
            tenor_vols['diff_exp'] = tenor_vols['diff_to_exp1'] + tenor_vols['diff_to_exp2']
           
            for idx, row in tenor_vols.iterrows():
                interp_vol1 = (1 - row['diff_to_exp1'] / row['diff_exp']) * vols.loc[idx, row['expiry1'].date()]
                interp_vol2 = (1 - row['diff_to_exp2'] / row['diff_exp']) * vols.loc[idx, row['expiry2'].date()]
                tenor_vols.loc[idx, field] = interp_vol1 + interp_vol2
                
        return tenor_vols[field]

    
if __name__ == "__main__":
    config = read_json('config.json')
    
    wrapper = InfluxDBWrapper(config['database']['url'], config['database']['token'], config['database']['org'], 30_000)

    smile = wrapper.get_smile_for_obs_time(bucket='eth_vol_surfaces',
                      measurement='volatility',
                      expiry='2023-09-29',
                      obs_time='2023-05-16T12:05:00Z',
                      field='mid_iv')
    
    forward_curve = wrapper.get_forward_curve_for_obs_time(bucket='eth_vol_surfaces',
                      measurement='volatility',
                      obs_time='2023-05-16T12:05:00Z')
    
    vol_surface = wrapper.get_vol_surface_for_obs_time(bucket='eth_vol_surfaces',
                      measurement='volatility',
                      obs_time='2023-05-16T12:05:00Z',
                      field='mid_iv')
    
    history_vol_for_delta_expiry = wrapper.get_historical_vol_for_delta_and_expiry(bucket='eth_vol_surfaces',
                      measurement='volatility',
                      range_start='2023-05-22T00:00:00Z',
                      range_end='2023-05-27T12:05:00Z',
                      delta='ATM',
                      expiry='2023-09-29',
                      field='mid_iv')
    
    history_vol_for_tenor = wrapper.get_historical_vol_for_tenor(bucket='eth_vol_surfaces',
                      measurement='volatility',
                      range_start='2023-05-22T00:00:00Z',
                      range_end='2023-05-27T00:00:00Z',
                      delta='ATM',
                      tenor='90D',
                      field='mid_iv')


    print(smile)
    print(forward_curve)
    print(vol_surface)
    print(history_vol_for_delta_expiry)
    print(history_vol_for_tenor)

