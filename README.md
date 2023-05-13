
# Deribit futrue and option order book loader

This program fetches and saves the option order book from Deribit exchange for given crypto-currencies to Parquet files using the Deribit API v2.   
It does not download the historical order books as it's not available through the API, but rather captures it live and builds an history continuously.   
It runs every 5mn but can be configured for other timeframes.   

Features:
- runs 20 concurrent threads to pull the data from Deribit
- supports multiple timeframes (min 5m as it takes between 1-3mn to download the data at every iteration)
- saves the data as parquet file 
- recommended to let it run on a vps to run continuously

## Installation

This program requires Python 3.6 or higher. 

## Usage

To run the program, execute the following command:

`python3 deribit_loader.py` 

The program will fetch and save the options data for the specified crypto-currencies to Parquet files in the `data` folder.

## Examples

To fetch and save the options data for BTC and ETH, run:

`python3 deribit_loader.py` 

## License

MIT License

## Additional Resources

-   [Deribit API documentation](https://docs.deribit.com/)
