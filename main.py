from src.logger import Logger
from twelvedata import TDClient
import pandas as pd
import logging
import os
from dotenv import load_dotenv
from src.utils import high_low_per_window, create_daily_date_schedule, create_overlapping_time_grid, shift_date_by_period
from src.configuration import Configuration
from src.fx_time_interval_scanner import FxTimeIntervalScanner



if __name__ == '__main__':
    # Load API keys
    load_dotenv()

    Logger()
    cfg = Configuration('run.cfg')

    scanner = FxTimeIntervalScanner(
        cfg.tick_interval,
        cfg.fx_rates[0],
        cfg.low_high_interval,
        cfg.historical_data_horizon)

    scanner.run_scanner()
    scanner.export_results()

    logging.info(f"Exiting run successfully")
