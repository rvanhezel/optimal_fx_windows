from src.logger import Logger
from twelvedata import TDClient
import pandas as pd
import logging
import os


def load_market_data(fx_cross,
                     tick_interval,
                     first_day,
                     last_day,
                     historical_data_filename) -> pd.DataFrame:
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
        data_file = os.path.join('data', historical_data_filename)
        logging.info(f'Reading data from csv: {data_file}')

        fx_data_df = pd.read_csv(data_file, index_col='datetime')
        fx_data_df.index = pd.to_datetime(fx_data_df.index)

    fx_data_df.index = pd.Series(fx_data_df.index)
    return fx_data_df