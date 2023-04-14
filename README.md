
# Deribit option order book loader

This program fetches and saves the option order book data for given crypto to Parquet files using the Deribit API v2.
It runs every 5mn but can be configured for other timeframes.
The provided jupyter notebook shows how to aggregate the data.

## Installation

This program requires Python 3.6 or higher. 

## Usage

To run the program, execute the following command:

Copy code

`python deribit_loader.py` 

The program will fetch and save the options data for the specified currencies to Parquet files in the `data` folder.

## Examples

To fetch and save the options data for BTC and ETH, run:

Copy code

`python deribit_loader.py` 

## License

MIT License

## Additional Resources

-   [Deribit API documentation](https://docs.deribit.com/)