from src.logger import Logger
from twelvedata import TDClient
import pandas as pd
import logging
import os
from dotenv import load_dotenv
from src.utils import high_low_per_window, create_daily_date_schedule, create_overlapping_time_grid, shift_date_by_period
from src.configuration import Configuration
from src.market_data_loader import load_market_data


class FxTimeIntervalScanner:

    def __init__(self, tick_interval, fx_rate, high_low_interval, historical_data_range):
        self.tick_interval = tick_interval
        self.fx_rate = fx_rate
        self.high_low_interval = high_low_interval
        self.historical_data_range = historical_data_range

        self.high_counter_df = None
        self.low_counter_df = None

        msg = f"Configured run with: {tick_interval} tick interval, "
        msg += f" {fx_rate} FX rate, {high_low_interval} horizon and "
        msg += f"{historical_data_range} historical data"
        logging.info(msg)

    def run_scanner(self):  
        # Define full date range 
        last_day = pd.Timestamp.today() - pd.Timedelta(days=3)
        first_day = shift_date_by_period(self.historical_data_range, last_day, "-")

        # Load market data
        fx_data_df = load_market_data(self.fx_rate,
                                    self.tick_interval,
                                    first_day,
                                    last_day)

        # Check first date from historical fx data. 
        # Shift by one day as first date is never full with TwelveData
        first_day = fx_data_df.index.min() + pd.Timedelta(days=1)
        logging.debug(f"Actual date range: {first_day.strftime('%Y-%m-%d')} to {last_day.strftime('%Y-%m-%d')}")

        # Compute the historical date schedule over which to compute the highs and lows,
        historical_period = create_daily_date_schedule(first_day, last_day)        

        # Define intra-day time windows
        start = pd.Timestamp(last_day.year, last_day.month, last_day.day, 0, 0)
        overlapping_intra_day_grid = create_overlapping_time_grid(
            start, 
            start + pd.Timedelta(days=1), 
            self.tick_interval, 
            self.high_low_interval,
            True)
                                                            
        # Initialize counters for time window distributions.
        # These are used to build the empirical probability distributions
        high_counter = {k: 0 for k in overlapping_intra_day_grid}
        low_counter = {k: 0 for k in overlapping_intra_day_grid}

        for current_date in historical_period:
            logging.debug(f'Computing {self.fx_rate} high and low windows for: {current_date.strftime('%Y-%m-%d')}')

            date_open = pd.Timestamp(current_date.year, current_date.month, current_date.day, 0, 0)
            date_close = date_open + pd.Timedelta(days=1)

            # Set current date time windows
            windows = create_overlapping_time_grid(
                date_open, 
                date_close, 
                self.tick_interval, 
                self.high_low_interval, 
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


        self.high_counter_df = pd.DataFrame(list(high_counter.items()), columns=['window', 'count'])
        self.low_counter_df = pd.DataFrame(list(low_counter.items()), columns=['window', 'count'])

    def export_results(self):
        if not os.path.exists('output'):
            os.makedirs('output')

        logging.debug(f"Exporting empirical distributions to csv")
        self.high_counter_df.to_csv(os.path.join('output', 'market_high_counter.csv'), index=False)
        self.low_counter_df.to_csv(os.path.join('output', 'market_low_counter.csv'), index=False)

        self.high_counter_df.sort_values(by='count', ascending=False, inplace=True)
        self.low_counter_df.sort_values(by='count', ascending=False, inplace=True)

        high_top3 = sorted(set(self.high_counter_df['count']), reverse=True)[:3]  # Sort in descending order and take the top 3
        low_top3 = sorted(set(self.low_counter_df['count']), reverse=True)[:3]  # Sort in descending order and take the top 3

        high_top_3_rows = pd.DataFrame()
        for value in high_top3:
            high_top_3_rows = pd.concat([high_top_3_rows, self.high_counter_df[self.high_counter_df['count'] == value]])
            if high_top_3_rows.shape[0] >= 3:
                break

        low_top_3_rows = pd.DataFrame()
        for value in low_top3:
            low_top_3_rows = pd.concat([low_top_3_rows, self.low_counter_df[self.low_counter_df['count'] == value]])
            if low_top_3_rows.shape[0] >= 3:
                break

        high_top_3_rows.to_csv(os.path.join('output', 'top_intervals_market_high.csv'), index=False)
        low_top_3_rows.to_csv(os.path.join('output', 'top_intervals_market_low.csv'), index=False)
