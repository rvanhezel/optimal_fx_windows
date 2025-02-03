from logger import Logger
from twelvedata import TDClient
import pandas as pd
import logging
import os
from dotenv import load_dotenv
from utils import high_low_per_window, create_daily_date_schedule, create_overlapping_time_grid


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


if __name__ == '__main__':
    # Load API keys
    load_dotenv()

    # Initialize logger
    log = Logger()

    tick_interval = 5 #min
    fx_cross = 'EUR/USD'
    high_low_interval = 90 #minutes
    historical_data_range = 1 #years - my current dataset is limited to approximately 1month of data

    msg = f"Configured run with: {tick_interval} minute tick interval, "
    msg += f" {fx_cross} FX rate, {high_low_interval} minute horizon and "
    msg += f"{historical_data_range} year historical data"
    logging.info(msg)

    # Define full date range 
    last_day = pd.Timestamp.today() - pd.Timedelta(days=3)
    first_day = last_day - pd.DateOffset(years=historical_data_range)

    # Load market data
    fx_data_df = load_market_data(fx_cross,
                                  tick_interval,
                                  first_day,
                                  last_day)

    # Check first date from historical fx data. 
    # Shift by one day as first date is never full with TwelveData
    first_day = fx_data_df.index.min() + pd.Timedelta(days=1)
    logging.debug(f"Date range: {first_day.strftime('%Y-%m-%d')} to {last_day.strftime('%Y-%m-%d')}")

    # Compute the historical date schedule over which to compute the highs and lows,
    # Currently over 1 year
    historical_period = create_daily_date_schedule(first_day, last_day)        

    # Define intra-day time windows
    start = pd.Timestamp(last_day.year, last_day.month, last_day.day, 0, 0)
    overlapping_intra_day_grid = create_overlapping_time_grid(
        start, 
        start + pd.Timedelta(days=1), 
        tick_interval, 
        high_low_interval,
        True)
                                                        
    # Initialize counters for time window distributions.
    # These are used to build the empirical probability distributions
    high_counter = {k: 0 for k in overlapping_intra_day_grid}
    low_counter = {k: 0 for k in overlapping_intra_day_grid}

    for current_date in historical_period:
        logging.debug(f'Computing high and low windows for: {current_date}')

        date_open = pd.Timestamp(current_date.year, current_date.month, current_date.day, 0, 0)
        date_close = date_open + pd.Timedelta(days=1)

        # Set current date time windows
        windows = create_overlapping_time_grid(
            date_open, 
            date_close, 
            tick_interval, 
            high_low_interval, 
            False)

        # Restrict dataset to current dates
        date_query = (fx_data_df.index >= date_open) & (fx_data_df.index < date_close)
        current_fx_data = fx_data_df[date_query]

        # Get high, lows across windows
        high_low_windows = pd.DataFrame([[
            window,
            *high_low_per_window(window, current_fx_data)] 
            for window in windows], 
            columns=["window", "low", "high"])

        max_value = high_low_windows['high'].max()
        found_high_windows = high_low_windows[high_low_windows['high'] == max_value]['window'].to_list()
        logging.debug(f"For {current_date.strftime('%Y-%m-%d')} found {len(found_high_windows)} windows containing the high")

        min_value = high_low_windows['low'].min()
        found_low_windows = high_low_windows[high_low_windows['low'] == min_value]['window'].to_list()
        logging.debug(f"For {current_date.strftime('%Y-%m-%d')}, found {len(found_low_windows)} windows containing the low")

        hw_as_time = [(
            window[0].to_pydatetime().time(),
            window[1].to_pydatetime().time(),
                    ) for window in found_high_windows]

        lw_as_time = [(
            window[0].to_pydatetime().time(),
            window[1].to_pydatetime().time(),
                    ) for window in found_low_windows]

        for high_window in hw_as_time:
            if high_window in high_counter:
                high_counter[high_window] += 1
            else:
                raise ValueError("Problem with time window")
            
        for low_window in lw_as_time:
            if low_window in low_counter:
                low_counter[low_window] += 1
            else:
                raise ValueError("Problem with time window")


    high_counter_df = pd.DataFrame(list(high_counter.items()), columns=['window', 'count'])
    low_counter_df = pd.DataFrame(list(low_counter.items()), columns=['window', 'count'])

    logging.debug(f"Exporting empirical distributions to csv")
    high_counter_df.to_csv(os.path.join('output', 'high_counter.csv'), index=False)
    low_counter_df.to_csv(os.path.join('output', 'low_counter.csv'), index=False)