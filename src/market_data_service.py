import os
import logging
import pandas as pd
from datetime import datetime
from typing import Optional
from src.mt5_api import MT5API
from src.configuration import Configuration
from src.period import Period


class MarketDataService:
    """Handles market data retrieval using either the MT5 API or local CSV files."""

    def __init__(self, 
                 config: Configuration,
                 mt5_login: int = None,
                 mt5_password: str = None,
                 mt5_server: str = None,
                 ) -> None:
        """
        Initializes the MarketDataHandler with a configuration object.

        :param config: Configuration object containing API credentials and settings.
        """
        self.use_api = config.use_api
        self.time_series_filename = config.historical_data_filename

        self.mt5_login = mt5_login
        self.mt5_password = mt5_password
        self.mt5_server = mt5_server

        self.mt5_api = None

        if self.use_api:
            if not all([self.mt5_login, self.mt5_password, self.mt5_server]):
                msg = "Login, password, and server must all be provided in the config"
                msg += " when using the MT5 API." 
                logging.error(msg)
                raise ValueError(msg)

            try:
                self.mt5_api = MT5API(self.mt5_login, self.mt5_password, self.mt5_server)
                logging.info("Successfully initialized MT5API.")
            except Exception as e:
                logging.error(f"Failed to initialize MT5API: {e}")
                raise

    def load_market_data(
        self,
        fx_cross: str, 
        tick_interval: Period, 
        first_day: datetime, 
        last_day: datetime
    ) -> Optional[pd.DataFrame]:
        """
        Loads market data either from the MT5 module or a CSV file.

        :param fx_cross: The currency pair symbol (e.g., "EURUSD").
        :param tick_interval: Period representing tick/time series frequency.
        :param first_day: Start date for historical data.
        :param last_day: End date for historical data.
        :return: A pandas DataFrame containing the historical data or None if an error occurs.
        """
        try:
            if self.use_api and self.mt5_api:
                logging.info(f"Fetching market data from MT5 API for {fx_cross} from {first_day} to {last_day}.")
                df = self.mt5_api.copy_rates_range(
                    fx_cross, 
                    tick_interval, 
                    first_day, 
                    last_day)

                if df is not None:
                    logging.info("Market data successfully retrieved from MT5 API.")
                else:
                    logging.warning("No data retrieved from MT5 API.")
                
                return df

            else:
                data_file = os.path.join('data', self.time_series_filename)
                logging.info(f'Reading data from csv: {data_file}')

                if not os.path.exists(data_file):
                    logging.error(f"CSV file {data_file} not found.")
                    return None

                df = pd.read_csv(data_file, index_col="datetime")
                df.index = pd.to_datetime(df.index)

                logging.info("Market data successfully loaded from CSV.")
                return df

        except Exception as e:
            logging.error(f"Error loading market data: {e}")
            return None

    def close(self) -> None:
        """Closes the MT5 API connection if in use."""
        if self.use_api and self.mt5_api:
            self.mt5_api.close()
            logging.info("MT5API connection closed.")
