from src.logger import Logger
from twelvedata import TDClient
import pandas as pd
import logging
import os
from dotenv import load_dotenv
from src.utils import high_low_per_window, create_daily_date_schedule, create_overlapping_time_grid, shift_date_by_period
from src.configuration import Configuration


def load_market_data(fx_cross: str = None,
                     tick_interval: str = None,
                     first_day: pd.Timestamp = None,
                     last_day: pd.Timestamp = None) -> pd.DataFrame:
    if False:    
        api_key = os.environ.get('TWELVE_DATA_KEY', 'WRONG-KEY')
        client_api = TDClient(apikey=api_key)

        logging.info(f'Calling API')
        fx_data_df = client_api.time_series(
            symbol=fx_cross,
            interval=tick_interval,
            start_date= first_day,	
            end_date=last_day,
            outputsize=min(count_points_between_dates(last_day, first_day, tick_interval), 5000)
        ).as_pandas()

        if not os.path.exists('data'):
            os.makedirs('data')

        fx_data_df.to_csv(os.path.join('data', f'fx_time_series.csv'))
    else:
        logging.info(f'Reading data from csv')
        fx_data_df = pd.read_csv(os.path.join('data', f'fx_time_series.csv'), index_col='datetime')
        fx_data_df.index = pd.to_datetime(fx_data_df.index)

    fx_data_df.index = pd.Series(fx_data_df.index)
    return fx_data_df