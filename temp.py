import pandas as pd
from market_data_builder import MarketDataBuilder
from datetime import datetime
from utils import datetime_to_unix_ms


# data = pd.read_parquet("data/Processed/eth_5m_20230611_options_data.parquet")
# data = data[(data['delta'] > -0.3) & (data['delta'] < -0.2)]


mdbuilder = MarketDataBuilder("data", 'eth')

data = mdbuilder.get_option_data("data/Processed/eth_5m_20230611_options_data.parquet")

# vol_surfaces, forward_curves = mdbuilder.extract_vol_surfaces_from_file("data/test.parquet")

obs_time1 = pd.to_datetime(datetime(2023, 6, 6, 6, 55, 0)).tz_localize('UTC')
obs_time2 = pd.to_datetime(datetime(2023, 6, 6, 7, 0, 0)).tz_localize('UTC')
obs_time3 = pd.to_datetime(datetime(2023, 6, 6, 7, 5, 0)).tz_localize('UTC')

# print(data.shape)
# print(data[(data['timestamp'] >= datetime_to_unix_ms(obs_time2)) & (data['timestamp'] < datetime_to_unix_ms(obs_time3))].shape)
# print(data[~((data['timestamp'] >= datetime_to_unix_ms(obs_time2)) & (data['timestamp'] < datetime_to_unix_ms(obs_time3)))].shape)

print(data)

raw_vol = data[(data['expiry'] + pd.to_timedelta('7:30:00') > obs_time2.tz_localize(None)) & (data['expiry'] == '2023-06-12') & (data['timestamp_datetime'] == '2023-06-11 13:00:00+00:00')][['cp', 'delta', 'bid_iv', 'mark_iv', 'ask_iv']].drop_duplicates(subset='delta')
# raw_vol = raw_vol[raw_vol['timestamp_datetime'] == '2023-06-11 13:00:00+00:00']
# keep only OTM options and with delta higher than 5%
raw_vol = raw_vol[(raw_vol['delta'] < 0.5) & (raw_vol['delta'] > -0.5) & (raw_vol['delta'].abs() > 0.05)]

print(raw_vol)
# smile_data = self.get_smile_for_expiry(raw_vol.to_numpy())
# data = mdbuilder.get_vol_surface_on_time(data, obs_time2)
# print(data)

# data = data[~((data['timestamp'] >= datetime_to_unix_ms(obs_time2)) & (data['timestamp'] < datetime_to_unix_ms(obs_time3)))]

# data[(data['timestamp'] >= datetime_to_unix_ms(obs_time2)) & (data['timestamp'] < datetime_to_unix_ms(obs_time3))].to_parquet('data/test.parquet')

# # print(obs_time)
# # print(pd.to_datetime(obs_time))
# # print(obs_time)
# # print(datetime_to_unix_ms(obs_time))
# data = mdbuilder.get_vol_surface_on_time(vol_surfaces, datetime_to_unix_ms(obs_time))
# # print(data)
# print(obs_time1)
# print(vol_surfaces[obs_time1])

# print(obs_time2)
# print(vol_surfaces[obs_time2])

# print(obs_time3)
# print(vol_surfaces[obs_time3])



# print(data.sort_values(by='mark_iv', ascending=False).head(10).transpose())