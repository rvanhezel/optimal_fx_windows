from dotenv import load_dotenv
import os
from src.period import Period
import configparser
import logging


class Configuration:

    def __init__(self, path_to_config: str):
        self.config = configparser.ConfigParser()
        self.config.read(path_to_config)

        self.log_level = self._configure_log(self.config.get('Run', 'log_level'))
        self.fx_rates = [key.strip() for key in self.config.get('Run', 'fx_rates').split(',')]
        self.spread = float(self.config.get('Run', 'spread'))

        logger = logging.getLogger()
        logger.setLevel(self.log_level)

        # Market data
        self.use_api = self.config.getboolean('MarketData', 'use_api')
        self.historical_data_filename = self.config.get('MarketData', 'historical_data_filename', fallback='fx_time_series.csv')
        self.market_data_timezone = self.config.get('MarketData', 'market_data_timezone')
        self.historical_data_horizon = Period(self.config.get('MarketData', 'historical_data_horizon'))
        self.tick_interval = Period(self.config.get('MarketData', 'tick_interval'))
        self.low_high_interval = Period(self.config.get('MarketData', 'low_high_interval'))
        self.market_open_time = self.config.get('MarketData', 'market_open_time')

        # Results
        self.full_results = self.config.getboolean('Results', 'full_results')

    def _configure_log(self, log_level: str):
        if log_level == "Debug":
            return logging.DEBUG
        elif log_level == "Info":
            return logging.INFO
        elif log_level == "Warning":
            return logging.WARNING
        elif log_level == "Error":
            return logging.ERROR
        else:
            raise ValueError("Log level not recognized")
