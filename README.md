
# Deribit volatility downloader and visualizer

This program fetches and saves the option order book from Deribit exchange for given crypto-currencies to Parquet files using the Deribit API v2.   
It does not download the historical order books as it's not available through the API, but rather captures it live and builds an history continuously.   
It runs every 5mn but can be configured for other timeframes.   

Features:
- runs 20 concurrent threads to pull the data from Deribit
- supports multiple timeframes (min 5m as it takes between 1-3mn to download the data at every iteration)
- saves the data as parquet files 
- build the volatility smiles (CubicSpline interpolation - SABR to come later)
- saves the volatility surfaces in InfluxDB
- saves the full order book in InfluxDB
- computes vol analytics like risk reversals, butterflies, eth vs btc diffs... and saves them in InfluxDB
- visualize the vol analytics with Grafana (dashboard provided in /Grafana)

## Installation

This program requires Python 3.6 or higher and an InfluxDB database if you wish to store the market data in a time-series DB. 
The following python modules are required:
- influxdb_client : python client to interact with InfluxDB database
- scipy : required for cubic spline interpolation of the smile
- tqdm : used to display the progression when processing the downloaded market data files

The dependancies can be installed via pip and the requirements file :

`pip install -r requirements.txt`


## Examples

### Market data capture

To start the download of order books from deribit:

`python3 deribit_loader.py` 

The program will fetch and save every 5mn the options and futures data for the specified crypto-currencies to Parquet files in the `data` folder. One day of data per file.
It is recommended to let it run on a vps to run continuously.


It is also recommended to run continuously market_data_builder.py which will process the data downloaded from deribit and saves them raw and processed in InfluxDB. 

`python3 market_data_builder.py`

### Market data construction


Volatility smiles are built on a sticky-delta basis ranging from 5-delta put to ATM and all the way to 5-delta call.
To build volatility smiles from the parquet files, run the following method from the MarketDataBuilder class :

`save_surfaces_to_influxdb('btc_vol_surfaces', file_pattern='btc_5m')`

This will save to the influxdb bucket 'btc_vol_surfaces' all the volatility smiles contained in the files in the data folder that match the pattern 'btc_5m'.
The file config.json needs to be properly filled with the influxdb information to run this method.

It's also possible the save the full order book in influxdb:

`save_order_book_to_influxdb('btc_deribit_order_book', 'btc_5m')`


The method save_rr_analytics_to_influxdb calculates risk reversals by tenor from the vol surfaces saved in 'bucket_source' and saves them in the bucket 'bucket_target'.
By default it runs for the last 4 days of data, it can be modified in the code.

`save_rr_analytics_to_influxdb(self, bucket_source, bucket_target)`

The method save_eth_vs_btc_analytics_to_influxdb calculates the vol diff between ETH and BTC by tenor and delta and saved them in 'bucket_target':

`save_eth_vs_btc_analytics_to_influxdb(self, bucket_source1, bucket_source2, bucket_target)`


### Volatility smile and surface visualization

The jupyter notebook 'vol_visualization' shows a few ways of looking at the volatility smile using a basic influxdb wrapper that pulls the volatility
data from the influxdb database.

3d volatility surface

![Vol surface](/pics/surface.JPG)


Volatility smile

![Vol smile](/pics/smile.JPG)


Volatility term structure

![Vol term structure](/pics/term_structure.JPG)


Historical implied volatility per delta and expiry

![Historical vol](/pics/historical_vol.JPG)


Historical risk reversal per delta and tenor

![Historical risk reversal](/pics/rr.JPG)


Historical butterfly per delta and tenor

![Historical butterfly](/pics/butterfly.JPG)


Risk-Reversal dashboard

![Risk-Reversal dashboard](/pics/rr_dashboard.JPG)


Grafana dashboard

![Grafana dashboard](/pics/grafana.JPG)
## License

MIT License

## Additional Resources

-   [Deribit API documentation](https://docs.deribit.com/)
-   [InfluxDB Linux installation](https://docs.influxdata.com/influxdb/v2.7/install/?t=Linux)
-   [InfluxDB-client-python](https://github.com/influxdata/influxdb-client-python)