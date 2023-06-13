
# Deribit volatility downloader and visualizer

This program fetches and saves the option order book from Deribit exchange for given crypto-currencies to Parquet files using the Deribit API v2.   
It does not download the historical order books as it's not available through the API, but rather captures it live and builds an history continuously.   
It runs every 5mn but can be configured for other timeframes.   

Features:
- runs 20 concurrent threads to pull the data from Deribit
- supports multiple timeframes (min 5m as it takes between 1-3mn to download the data at every iteration)
- saves the data as parquet files 
- it is recommended to let it run on a vps to run continuously
- build the volatility smiles (CubicSpline interpolation - SABR to come later)
- saves the volatility surfaces in InfluxDB
- saves the full order book in InfluxDB (heavy size)

## Installation

This program requires Python 3.6 or higher and an InfluxDB database if you wish to store the market data in a time-series DB. 
The following python modules are required:
- influxdb_client : python client to interact with InfluxDB database
- scipy : required for cubic spline interpolation of the smile
- tqdm : used to display the progression when processing the downloaded market data files


## Usage

To start the download of order books from deribit:

`python3 deribit_loader.py` 

The program will fetch and save every 5mn the options and futures data for the specified crypto-currencies to Parquet files in the `data` folder. One day of data per file.


## Examples

### Market data construction


Volatility smiles are built on a sticky-delta basis ranging from 5-delta put to ATM and all the way to 5-delta call.
To build volatility smiles from the parquet files, run the following method from the MarketDataBuilder class :

`save_surfaces_to_influxdb('btc_vol_surfaces', file_pattern='btc_5m')`

This will save to the influxdb bucket 'btc_vol_surfaces' all the volatility smiles contained in the files in the data folder that match the pattern 'btc_5m'.
The file config.json needs to be properly filled with the influxdb information to run this method.

It's also possible the save the full order book in influxdb although it's heavy on the disk:

`save_order_book_to_influxdb('btc_deribit_order_book', 'btc_5m')`

Once a file is processed successfully by one of the above methods, it is moved to the Processed folder.


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
## License

MIT License

## Additional Resources

-   [Deribit API documentation](https://docs.deribit.com/)
-   [InfluxDB Linux installation](https://docs.influxdata.com/influxdb/v2.7/install/?t=Linux)
-   [InfluxDB-client-python](https://github.com/influxdata/influxdb-client-python)