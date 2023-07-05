from influxdb_client import InfluxDBClient
import pandas as pd
from datetime import datetime, timedelta

from utils import read_json, build_option_expiries, add_tenor, timeit, convert_to_deribit_date



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

    def _write_influx_field(self, arg, arg_name, regex = False):
        """
        Write the InfluxDB field for a given argument.

        Args:
            arg: The argument value.
            arg_name (str): The argument name.

        Returns:
            str: The formatted InfluxDB field.
        """
        if not isinstance(arg, list):
            # r_arg = f'"{arg}"'
            return self._write_influx_field([arg], arg_name, regex)
        else:
            if regex == False:
                r_arg = f'"{arg[0]}"'
                for a in arg[1:]:
                    r_arg = f'{r_arg} or r.{arg_name} == "{a}"'
            else:
                r_arg = f'/{arg[0]}/'
                for a in arg[1:]:
                    r_arg = f'{r_arg} or r.{arg_name} =~ /{a}/'

        return r_arg

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
        r_field = self._write_influx_field(field, '_field') 
        r_expiry = self._write_influx_field(expiry, 'expiry')
        with InfluxDBClient(url=self.url, token=self.token, org=self.org, timeout=self.timeout) as client:
            query = f'from(bucket: "{bucket}")\
                        |> range(start: {obs_time}, stop: {obs_time.replace("0Z", "1Z")})\
                        |> filter(fn: (r) => r._measurement == "{measurement}") \
                        |> filter(fn: (r) => r.expiry == {r_expiry})\
                        |> filter(fn: (r) => r._time == {obs_time})\
                        |> filter(fn: (r) => r._field == {r_field})'

            result = client.query_api().query(query)

            return pd.DataFrame(data=result.to_values(columns=['delta', '_value', '_field', 'expiry']), columns=['delta', 'value', 'field', 'expiry'])

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
  
    def get_historical_vol_for_delta_and_expiry(self, bucket, measurement, range_start, range_end, delta, expiry, field, timeframe = False):
        """
        Retrieves the historical volatility data for a specific delta and expiry range.

        Args:
            bucket (str): The name of the InfluxDB bucket.
            measurement (str): The measurement name in the InfluxDB.
            range_start (str): The start of the range for retrieving historical data.
            range_end (str): The end of the range for retrieving historical data.
            delta (str/list): The delta value(s) for the options.
            expiry (str/list): The expiration date(s) of the options.
            field (str/list): The field(s) to retrieve from the data.
            timeframe (bool or str, optional): The timeframe to aggregate the data. Defaults to False.

        Returns:
            pandas.DataFrame: The historical volatility data for the specified delta and expiry range.
        """
        r_delta = self._write_influx_field(delta, 'delta')
        r_expiry = self._write_influx_field(expiry, 'expiry')
        r_field = self._write_influx_field(field, '_field')

        with InfluxDBClient(url=self.url, token=self.token, org=self.org, timeout=self.timeout) as client:
            query = f'from(bucket: "{bucket}")\
                        |> range(start: {range_start}, stop: {range_end})\
                        |> filter(fn: (r) => r._measurement == "{measurement}") \
                        |> filter(fn: (r) => r.expiry == {r_expiry})\
                        |> filter(fn: (r) => r.delta == {r_delta})\
                        |> filter(fn: (r) => r._field == {r_field})'
            
            if timeframe != False:
                query = f'{query}\n|> aggregateWindow(every: {timeframe}, fn: last, createEmpty: false)'

            result = client.query_api().query(query)

            return pd.DataFrame(data=result.to_values(columns=['_time', '_value', '_field', 'delta', 'expiry']), columns=['timestamp', 'value', 'field', 'delta', 'expiry'])
        
        
    def get_historical_vol_for_strike_and_expiry(self, bucket, measurement, range_start, range_end, strike, expiry, field, timeframe = False, include_greeks = False):
        """
        Retrieves the historical volatility data for a specific delta and expiry range.

        Args:
            bucket (str): The name of the InfluxDB bucket.
            measurement (str): The measurement name in the InfluxDB.
            range_start (str): The start of the range for retrieving historical data.
            range_end (str): The end of the range for retrieving historical data.
            strike (str/list[str]): The strike(s) of the options.
            expiry (str/list[str]): The expiration date(s) of the options.
            field (str/list[str]): The field(s) to retrieve from the data.
            timeframe (bool or str, optional): The timeframe to aggregate the data. Defaults to False.
            include_greeks (bool): The sensitivities (greeks) of the given option(s).

        Returns:
            pandas.DataFrame: The historical volatility data for the specified delta and expiry.
        """
        if include_greeks == True:
            field = field + ['delta', 'gamma', 'rho', 'theta', 'vega']
        r_field = self._write_influx_field(field, '_field')

        instruments = []
        if not isinstance(expiry, list):
            expiry = [expiry]
        if not isinstance(strike, list):
            strike = [strike]
        for exp in expiry:
            for s in strike:
                instruments.append(f"{convert_to_deribit_date(exp)}-{s}-C")
                instruments.append(f"{convert_to_deribit_date(exp)}-{s}-P")

        r_instrument = self._write_influx_field(instruments, 'instrument_name', regex=True)

        with InfluxDBClient(url=self.url, token=self.token, org=self.org, timeout=self.timeout) as client:
            query = f'from(bucket: "{bucket}")\
                        |> range(start: {range_start}, stop: {range_end})\
                        |> filter(fn: (r) => r._measurement == "{measurement}") \
                        |> filter(fn: (r) => r.instrument_name =~ {r_instrument})  \
                        |> filter(fn: (r) => r._field == {r_field})'

            if timeframe != False:
                query = f'{query}\n|> aggregateWindow(every: {timeframe}, fn: last, createEmpty: false)'

            result = client.query_api().query(query)

            return pd.DataFrame(data=result.to_values(columns=['_time', '_value', '_field', 'instrument_name']), columns=['timestamp', 'value', 'field', 'instrument_name'])

    def get_historical_vol(self, bucket, measurement, range_start, range_end, field, timeframe = False):
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
        r_field = self._write_influx_field(field, '_field')
        with InfluxDBClient(url=self.url, token=self.token, org=self.org, timeout=self.timeout) as client:
            query = f'from(bucket: "{bucket}")\
                        |> range(start: {range_start}, stop: {range_end})\
                        |> filter(fn: (r) => r._measurement == "{measurement}") \
                        |> filter(fn: (r) => r._field == {r_field})'
            
            if timeframe != False:
                query = f'{query}\n|> aggregateWindow(every: {timeframe}, fn: last, createEmpty: false)'

            result = client.query_api().query(query)

            return pd.DataFrame(data=result.to_values(columns=['_time', '_value', '_field', 'expiry', 'delta']), columns=['timestamp', 'value', 'field', 'expiry', 'delta'])
    
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

        current_utc_time = datetime.utcnow()
        result = []
        current_day = range_start
        while current_day <= range_end:
            # if end date is today and time is before 11am UTC, expiry roll hasn't happened yet
            if range_end == current_utc_time.date() and current_utc_time.hour < 11:
                day_expiries = build_option_expiries(current_day - timedelta(days=1))
            else:
                day_expiries = build_option_expiries(current_day)

            current_tenor = add_tenor(current_day, tenor)

            expiry1 = day_expiries[0]
            expiry2 = day_expiries[1]
            idx = 1
            while expiry2 < current_tenor:
                if idx == len(day_expiries) - 1:
                    expiry1 = day_expiries[idx]
                    expiry2 = expiry1
                    break
                
                expiry1 = day_expiries[idx]
                expiry2 = day_expiries[idx+1]
                idx += 1
            result.append([current_day, expiry1, expiry2])
             
            current_day += timedelta(days=1)

        result = [[date.strftime("%Y-%m-%d"), expiry1.strftime("%Y-%m-%d"), expiry2.strftime("%Y-%m-%d")] for date, expiry1, expiry2 in result]

        return pd.DataFrame(data=result, columns=['date', 'expiry1', 'expiry2']).set_index('date')
 
    def get_historical_vol_for_delta_and_tenor(self, bucket, measurement, range_start, range_end,
                                                delta, tenor, field, timeframe = False):
        """
        Retrieves the historical volatility data for a specific tenor and delta.

        Args:
            bucket (str): The name of the InfluxDB bucket.
            measurement (str): The measurement name in the InfluxDB.
            range_start (str): The start of the range for retrieving historical data.
            range_end (str): The end of the range for retrieving historical data.
            delta (list[str]/str): The delta value(s) for the options.
            tenor (list[str]/str): The tenor value(s) for the options.
            field (list[str]/str): The field(s) to retrieve from the data. Default is mid_iv.

        Returns:
            pandas.DataFrame: The historical volatility data for the specified tenor.
        """
        if not isinstance(delta, list):
            delta = [delta]
        if not isinstance(tenor, list):
            tenor = [tenor]
        if not isinstance(field, list):
            field = [field]

        unique_expiries = pd.Series()
        for t in tenor:
            nearby_expiries = self._get_historical_nearby_expiries_for_tenor(range_start, range_end, t)
            nearby_expiries.index = pd.to_datetime(nearby_expiries.index)  
            unique_expiries = pd.concat([unique_expiries, nearby_expiries['expiry1'], nearby_expiries['expiry2']], axis=0)

        unique_expiries = list(unique_expiries.unique())

        result_df = self.get_historical_vol_for_delta_and_expiry(bucket=bucket,
                                                        measurement=measurement,
                                                        range_start=range_start,
                                                        range_end=range_end,
                                                        delta=delta,
                                                        expiry=unique_expiries,
                                                        field=field,
                                                        timeframe=timeframe)

        final_vols = pd.DataFrame()
        for d in delta:
            for f in field:
                for t in tenor:
                    nearby_expiries = self._get_historical_nearby_expiries_for_tenor(range_start, range_end, t)
                    vols = result_df[(result_df['delta'] == d) & (result_df['field'] == f)].pivot(index='timestamp', columns='expiry', values='value').tz_localize(None).ffill()

                    nearby_expiries.index = pd.to_datetime(nearby_expiries.index)
                    nearby_expiries = nearby_expiries.reindex(vols.index, method='ffill') 
                    
                    tenor_vols = pd.DataFrame(index=vols.index)

                    tenor_vols['expiry1'] = pd.to_datetime(nearby_expiries['expiry1'])
                    tenor_vols['expiry2'] = pd.to_datetime(nearby_expiries['expiry2'])
                    tenor_vols['rolling_expiry'] = tenor_vols.index.map(lambda x: add_tenor(x, tenor=t))

                    tenor_vols['diff_to_exp1'] = abs((tenor_vols['expiry1'] - tenor_vols['rolling_expiry']).dt.days)
                    tenor_vols['diff_to_exp2'] = abs((tenor_vols['expiry2'] - tenor_vols['rolling_expiry']).dt.days)
                    tenor_vols['diff_exp'] = tenor_vols['diff_to_exp1'] + tenor_vols['diff_to_exp2']

                    # workaround to handle case where some short term illiquid expiries have been excluded by the scrapper but are required for the calcs
                    # will need to be refactored 
                    for index, row in tenor_vols.iterrows():
                        if row['expiry1'] not in vols.columns:
                            closest_expiry = min(vols.columns, key=lambda x: abs((datetime.strptime(x, '%Y-%m-%d') - row['expiry1']).days))
                            tenor_vols.at[index, 'expiry1'] = closest_expiry

                        if row['expiry2'] not in vols.columns:
                            closest_expiry = min(vols.columns, key=lambda x: abs((datetime.strptime(x, '%Y-%m-%d') - row['expiry2']).days))
                            tenor_vols.at[index, 'expiry2'] = closest_expiry
                
                    for idx, row in tenor_vols.iterrows():
                        interp_vol1 = (1 - row['diff_to_exp1'] / row['diff_exp']) * vols.loc[idx, row['expiry1'].strftime("%Y-%m-%d")]
                        interp_vol2 = (1 - row['diff_to_exp2'] / row['diff_exp']) * vols.loc[idx, row['expiry2'].strftime("%Y-%m-%d")]
                        tenor_vols.loc[idx, 'value'] = interp_vol1 + interp_vol2
                    
                    tenor_vols['value'] = tenor_vols['value'].ffill()
                    tenor_vols['delta'] = d
                    tenor_vols['field'] = f
                    tenor_vols['tenor'] = t
                    final_vols = pd.concat([final_vols, pd.DataFrame(data=tenor_vols[['value', 'field', 'delta', 'tenor']], columns=['value', 'field', 'delta', 'tenor'])], axis = 0)

        return final_vols.reset_index()
    

    def get_historical_risk_reversal_by_delta_and_tenor(self, bucket, measurement, range_start, range_end, 
                                                        delta, tenor, field='mid_iv', normalize_by_ATM=False, timeframe = False):
        """
        Retrieves the historical risk reversal data for a specific delta and tenor.

        Args:
            bucket (str): The name of the InfluxDB bucket.
            measurement (str): The measurement name in the InfluxDB.
            range_start (str): The start of the range for retrieving historical data.
            range_end (str): The end of the range for retrieving historical data.
            delta (list[str]/str): The delta value(s) for the options.
            tenor (list[str]/str): The tenor value(s) for the options.
            field (list[str]/str): The field(s) to retrieve from the data. Default is mid_iv.
            normalize_by_ATM (bool): Flag indicating whether to normalize the data by ATM. Default is False.
            timeframe (str/bool): Timeframe to be requested from InfluxDB. Let InfluxDB decide if False.

        Returns:
            pandas.DataFrame: The historical risk reversal data for the specified delta and tenor.
        """
        if not isinstance(delta, list):
            delta = [delta]
        if not isinstance(tenor, list):
            tenor = [tenor]
        if not isinstance(field, list):
            field = [field]

        deltas = []
        for d in delta:    
            deltas.append(f"{d}C")
            deltas.append(f"{d}P")
        if normalize_by_ATM == True:
            deltas.append('ATM')

        history = self.get_historical_vol_for_delta_and_tenor(bucket=bucket,
                      measurement=measurement,
                      range_start=range_start,
                      range_end=range_end,
                      delta=deltas,
                      tenor=tenor,
                      field=field,
                      timeframe=timeframe).set_index('timestamp')

        final_vols = pd.DataFrame()
        for d in delta:
            for f in field:
                for t in tenor:
                    mask = (history['tenor'] == t) & (history['field'] == f)
                    res = pd.DataFrame()
                    res['value'] = (history[(mask) & (history['delta'] ==  f"{d}C")]['value'] - history[(mask) & (history['delta'] == f"{d}P")]['value'])
                    if normalize_by_ATM == True:
                        res['value'] = res['value'] / history[(mask) & (history['delta'] == 'ATM')]['value']
                    res['delta'] = d
                    res['field'] = f
                    res['tenor'] = t
                    
                    final_vols = pd.concat([final_vols, pd.DataFrame(data=res[['value', 'field', 'delta', 'tenor']], columns=['value', 'field', 'delta', 'tenor'])], axis = 0)
        
        return final_vols.reset_index()

    def get_historical_vol_diff_by_delta_and_tenor(self, bucket_ccy1, bucket_ccy2, measurement, range_start, range_end, 
                                                        delta, tenor, field='mid_iv', timeframe = False):
        """
        Retrieves the historical risk reversal data for a specific delta and tenor.

        Args:
            bucket_ccy1 (str): The name of the InfluxDB bucket for the first currency.
            bucket_ccy2 (str): The name of the InfluxDB bucket for the second currency.
            measurement (str): The measurement name in the InfluxDB.
            range_start (str): The start of the range for retrieving historical data.
            range_end (str): The end of the range for retrieving historical data.
            delta (list[str]/str): The delta value(s) for the options.
            tenor (list[str]/str): The tenor value(s) for the options.
            field (list[str]/str): The field(s) to retrieve from the data. Default is mid_iv.
            normalize_by_ATM (bool): Flag indicating whether to normalize the data by ATM. Default is False.
            timeframe (str/bool): Timeframe to be requested from InfluxDB. Let InfluxDB decide if False.

        Returns:
            pandas.DataFrame: The historical vol differential data for the specified delta and tenor.
        """
        if not isinstance(delta, list):
            delta = [delta]
        if not isinstance(tenor, list):
            tenor = [tenor]
        if not isinstance(field, list):
            field = [field]


        history_ccy1 = self.get_historical_vol_for_delta_and_tenor(bucket=bucket_ccy1,
                      measurement=measurement,
                      range_start=range_start,
                      range_end=range_end,
                      delta=delta,
                      tenor=tenor,
                      field=field,
                      timeframe=timeframe).set_index('timestamp')
        
        history_ccy2 = self.get_historical_vol_for_delta_and_tenor(bucket=bucket_ccy2,
                      measurement=measurement,
                      range_start=range_start,
                      range_end=range_end,
                      delta=delta,
                      tenor=tenor,
                      field=field,
                      timeframe=timeframe).set_index('timestamp')

        final_vols = pd.DataFrame()
        for d in delta:
            for f in field:
                for t in tenor:
                    mask1 = (history_ccy1['tenor'] == t) & (history_ccy1['field'] == f)
                    mask2 = (history_ccy2['tenor'] == t) & (history_ccy2['field'] == f)
                    res = pd.DataFrame()
                    res['value'] = (history_ccy1[(mask1) & (history_ccy1['delta'] ==  f"{d}")]['value'] - history_ccy2[(mask2) & (history_ccy1['delta'] == f"{d}")]['value'])
                    res['delta'] = d
                    res['field'] = f
                    res['tenor'] = t
                    
                    final_vols = pd.concat([final_vols, pd.DataFrame(data=res[['value', 'field', 'delta', 'tenor']], columns=['value', 'field', 'delta', 'tenor'])], axis = 0)
        
        return final_vols.reset_index()

    def get_historical_butterfly_by_delta_and_tenor(self, bucket, measurement, range_start, range_end, 
                                                        delta, tenor, field='mid_iv', timeframe = False):
        """
        Retrieves the historical butterfly data for a specific delta and tenor.

        Args:
            bucket (str): The name of the InfluxDB bucket.
            measurement (str): The measurement name in the InfluxDB.
            range_start (str): The start of the range for retrieving historical data.
            range_end (str): The end of the range for retrieving historical data.
            delta (list[str]/str): The delta value(s) for the options.
            tenor (list[str]/str): The tenor value(s) for the options.
            field (list[str]/str): The field(s) to retrieve from the data. Default is mid_iv.
            timeframe (str/bool): Timeframe to be requested from InfluxDB. Let InfluxDB decide if False.

        Returns:
            pandas.DataFrame: The historical butterfly data for the specified delta and tenor.
        """
        if not isinstance(delta, list):
            delta = [delta]
        if not isinstance(tenor, list):
            tenor = [tenor]
        if not isinstance(field, list):
            field = [field]

        deltas = []
        for d in delta:    
            deltas.append(f"{d}C")
            deltas.append(f"{d}P")
        deltas.append('ATM')

        history = self.get_historical_vol_for_delta_and_tenor(bucket=bucket,
                      measurement=measurement,
                      range_start=range_start,
                      range_end=range_end,
                      delta=deltas,
                      tenor=tenor,
                      field=field,
                      timeframe=timeframe).set_index('timestamp')        
        
        final_vols = pd.DataFrame()
        for d in delta:
            for f in field:
                for t in tenor:
                    res = pd.DataFrame()
                    mask = (history['tenor'] == t) & (history['field'] == f)
                    res['value'] = history[(mask) & (history['delta'] == f"{d}C")]['value'] + history[(mask) & (history['delta'] == f"{d}P")]['value'] \
                            - 2 * history[(mask) & (history['delta'] == 'ATM')]['value']
                    res['delta'] = d
                    res['field'] = f
                    res['tenor'] = t

                    final_vols = pd.concat([final_vols, pd.DataFrame(data=res[['value', 'field', 'delta', 'tenor']], columns=['value', 'field', 'delta', 'tenor'])], axis = 0)

        return final_vols.reset_index()

    
if __name__ == "__main__":
    config = read_json('config.json')
    
    wrapper = InfluxDBWrapper(config['database']['url'], config['database']['token'], config['database']['org'], 30_000)

    smile = wrapper.get_smile_for_obs_time(bucket='eth_vol_surfaces',
                      measurement='volatility',
                      expiry=['2023-09-29','2023-12-29'],
                      obs_time='2023-05-16T12:05:00Z',
                      field=['mid_iv', 'bid_iv', 'ask_iv'])
    print(smile)
    
    forward_curve = wrapper.get_forward_curve_for_obs_time(bucket='eth_vol_surfaces',
                      measurement='volatility',
                      obs_time='2023-05-16T12:05:00Z')
    print(forward_curve)
    
    vol_surface = wrapper.get_vol_surface_for_obs_time(bucket='eth_vol_surfaces',
                      measurement='volatility',
                      obs_time='2023-05-16T12:05:00Z',
                      field='mid_iv')
    print(vol_surface)

    history_vol = wrapper.get_historical_vol(bucket='eth_vol_surfaces',
                      measurement='volatility',
                      range_start='2023-06-24T00:00:00Z',
                      range_end='2023-06-29T05:05:00Z',
                      field=['mid_iv'],
                      timeframe='4h')
    print(history_vol)
    
    history_vol_for_delta_expiry = wrapper.get_historical_vol_for_delta_and_expiry(bucket='eth_vol_surfaces',
                      measurement='volatility',
                      range_start='2023-05-10T00:00:00Z',
                      range_end='2023-05-27T12:05:00Z',
                      delta=['ATM', '15C', '25P'],
                      expiry=['2023-09-29','2023-12-29'],
                      field=['bid_iv', 'mid_iv'],
                      timeframe='4h')
    print(history_vol_for_delta_expiry)

    history_vol_for_strike_expiry = wrapper.get_historical_vol_for_strike_and_expiry(bucket='eth_deribit_order_book',
                      measurement='order_book',
                      range_start='2023-05-10T00:00:00Z',
                      range_end='2023-06-16T12:05:00Z',
                      strike=[1400, 1900],
                      expiry=['2023-07-28','2023-12-29'],
                      field=['mark_price', 'mark_iv'],
                      timeframe='4h',
                      include_greeks=True)
    print(history_vol_for_strike_expiry)

    history_vol_for_tenor = wrapper.get_historical_vol_for_delta_and_tenor(bucket='eth_vol_surfaces',
                      measurement='volatility',
                      range_start='2023-05-24T00:00:00Z',
                      range_end='2023-06-28T00:00:00Z',
                      delta=['5C', 'ATM'],
                      tenor=['7D', '1M'],
                      field='mid_iv',
                      timeframe='4h')
    print(history_vol_for_tenor)

    history_vol_diff_for_tenor = wrapper.get_historical_vol_diff_by_delta_and_tenor(bucket_ccy1='eth_vol_surfaces',
                      bucket_ccy2='btc_vol_surfaces',
                      measurement='volatility',
                      range_start='2023-06-01T00:00:00Z',
                      range_end='2023-06-20T00:00:00Z',
                      delta=['15C', 'ATM'],
                      tenor=['7D', '90D'],
                      field='mid_iv',
                      timeframe='1h')
    print(history_vol_diff_for_tenor)
    
    history_risk_reversal = wrapper.get_historical_risk_reversal_by_delta_and_tenor(bucket='eth_vol_surfaces',
                      measurement='volatility',
                      range_start='2023-05-25T00:00:00Z',
                      range_end='2023-06-05T09:00:00Z',
                      delta=15,
                      tenor=['14D', '90D'],
                      field=['bid_iv', 'mid_iv'],
                      timeframe='15m')
    print(history_risk_reversal)
    
    history_flies = wrapper.get_historical_butterfly_by_delta_and_tenor(bucket='btc_vol_surfaces',
                      measurement='volatility',
                      range_start='2023-06-01T00:00:00Z',
                      range_end='2023-06-06T15:35:00Z',
                      delta=[15, 25, 35],
                      tenor=['14d', '1M'],
                      field='mid_iv',
                      timeframe='15m')
    print(history_flies)