import pandas as pd
import logging
import os
from src.utils import high_low_per_window, create_daily_date_schedule, create_overlapping_time_grid, shift_date_by_period
from src.market_data_service import MarketDataService


class FxTimeIntervalScanner:

    def __init__(self, 
                 tick_interval, 
                 fx_rate, 
                 high_low_interval, 
                 historical_data_range,
                 historical_data_filename,
                 market_data_service: MarketDataService,
                 spread=0.0,
                 market_data_timezone=None,
                 ref_timezone=None,
                 market_open_time: str = None):
        self.tick_interval = tick_interval
        self.fx_rate = fx_rate
        self.high_low_interval = high_low_interval
        self.historical_data_range = historical_data_range
        self.historical_data_filename = historical_data_filename
        self.spread = spread
        self.market_data_service = market_data_service
        self.timezone = market_data_timezone
        self.market_open_time = market_open_time
        self.ref_timezone = ref_timezone
        
        self.high_counter_df = None
        self.low_counter_df = None
        self.low_or_high_counter_df = None

        msg = f"Configured run with: {tick_interval} tick interval, "
        msg += f" {fx_rate} FX rate, {high_low_interval} horizon and "
        msg += f"{historical_data_range} historical data"
        logging.info(msg)

    def run_scanner(self):  
        # Define full date range for when using API
        last_day = pd.Timestamp.today(tz=self.ref_timezone) - pd.Timedelta(days=1)
        first_day = shift_date_by_period(self.historical_data_range, last_day, "-")

        # Load market data
        fx_data_df = self.market_data_service.load_market_data(
            self.fx_rate,
            self.tick_interval,
            first_day.to_pydatetime(),
            last_day.to_pydatetime())
        
        # Adjust for timezone difference
        fx_data_df.index = fx_data_df.index.tz_localize(self.timezone)
        fx_data_df.to_csv(os.path.join('output', self.fx_rate + '_raw_time_series.csv'))

        fx_data_df.index = fx_data_df.index.tz_convert(self.ref_timezone)


        # Check first and last date from loaded fx data. 
        # Shift by one day as first date is never full with TwelveData
        first_day = fx_data_df.index.min() + pd.Timedelta(days=1)
        last_day = fx_data_df.index.max() + pd.Timedelta(days=1)
        logging.info(f"Actual date range: {first_day.strftime('%Y-%m-%d')} to {last_day.strftime('%Y-%m-%d')}")

        # Compute the historical date schedule over which to compute the highs and lows,
        historical_period = create_daily_date_schedule(first_day, last_day)        

        # Define intra-day time windows
        start = pd.Timestamp(
            last_day.year, 
            last_day.month, 
            last_day.day, 
            int(self.market_open_time[:2]), 
            int(self.market_open_time[2:]),
            tz=self.ref_timezone)
        
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
        low_or_high_counter = {k: 0 for k in overlapping_intra_day_grid}
        

        for current_date in historical_period:
            logging.info(f'Computing {self.fx_rate} high and low windows for: {current_date.strftime('%Y-%m-%d')}')

            date_open = pd.Timestamp(current_date.year, 
                                     current_date.month, 
                                     current_date.day, 
                                     int(self.market_open_time[:2]), 
                                     int(self.market_open_time[2:]), 
                                     tz=self.ref_timezone)
            date_close = date_open + pd.Timedelta(days=1)
            logging.info(f'Trading day: {date_open} to {date_close} {self.ref_timezone}')


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

            daily_high = current_fx_data['high'].max() - self.spread
            found_high_windows = high_low_windows[high_low_windows['high'] >= daily_high]['window'].to_list()
            logging.debug(f"For {current_date.strftime('%Y-%m-%d')} found {len(found_high_windows)} windows containing the high")

            daily_low = current_fx_data['low'].min() + self.spread
            found_low_windows = high_low_windows[high_low_windows['low'] <= daily_low]['window'].to_list()
            logging.debug(f"For {current_date.strftime('%Y-%m-%d')}, found {len(found_low_windows)} windows containing the low")

            low_and_high_query = (high_low_windows['high'] >= daily_high) | (high_low_windows['low'] <= daily_low)
            found_low_high_windows = high_low_windows[low_and_high_query]['window'].to_list()
            logging.debug(f"For {current_date.strftime('%Y-%m-%d')}, found {len(found_low_high_windows)} windows containing the low or high")

            hw_as_time = [(
                window[0].to_pydatetime().time(),
                window[1].to_pydatetime().time(),
                        ) for window in found_high_windows]

            lw_as_time = [(
                window[0].to_pydatetime().time(),
                window[1].to_pydatetime().time(),
                        ) for window in found_low_windows]
            
            low_or_high_as_time = [(
                window[0].to_pydatetime().time(),
                window[1].to_pydatetime().time(),
                        ) for window in found_low_high_windows]

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
                
            for low_or_high_window in low_or_high_as_time:
                if low_or_high_window in low_or_high_counter:
                    low_or_high_counter[low_or_high_window] += 1
                else:
                    raise ValueError("Problem with time window")

        self.high_counter_df = pd.DataFrame(list(high_counter.items()), columns=['window', 'count'])
        self.high_counter_df['probability'] = self.high_counter_df['count'] / len(historical_period)

        self.low_counter_df = pd.DataFrame(list(low_counter.items()), columns=['window', 'count'])
        self.low_counter_df['probability'] = self.low_counter_df['count'] / len(historical_period)

        self.low_or_high_counter_df = pd.DataFrame(list(low_or_high_counter.items()), columns=['window', 'count'])
        self.low_or_high_counter_df['probability'] = self.low_or_high_counter_df['count'] / len(historical_period)

    def export_results(self, full_results: bool) -> None:
        if not os.path.exists('output'):
            os.makedirs('output')

        prefix = self.fx_rate + '_'
        logging.info(f"Exporting empirical distributions to csv")
        if full_results:
            self.high_counter_df.to_csv(os.path.join('output', prefix + 'market_high_counter.csv'), index=False)
            self.low_counter_df.to_csv(os.path.join('output', prefix + 'market_low_counter.csv'), index=False)
            self.low_or_high_counter_df.to_csv(os.path.join('output', prefix + 'market_low_or_high_counter.csv'), index=False)

        self.high_counter_df.sort_values(by='count', ascending=False, inplace=True)
        self.low_counter_df.sort_values(by='count', ascending=False, inplace=True)
        self.low_or_high_counter_df.sort_values(by='count', ascending=False, inplace=True)

        high_top3 = sorted(set(self.high_counter_df['count']), reverse=True)[:3]  
        low_top3 = sorted(set(self.low_counter_df['count']), reverse=True)[:3]  
        low_or_high_top3 = sorted(set(self.low_or_high_counter_df['count']), reverse=True)[:3]  

        high_top_3_rows = pd.DataFrame()
        for value in high_top3:
            high_top_3_rows = pd.concat([
                high_top_3_rows, 
                self.high_counter_df[self.high_counter_df['count'] == value]])
            if high_top_3_rows.shape[0] >= 3:
                break

        low_top_3_rows = pd.DataFrame()
        for value in low_top3:
            low_top_3_rows = pd.concat([
                low_top_3_rows, 
                self.low_counter_df[self.low_counter_df['count'] == value]])
            if low_top_3_rows.shape[0] >= 3:
                break

        low_or_high_top3_rows = pd.DataFrame()
        for value in low_or_high_top3:
            low_or_high_top3_rows = pd.concat([
                low_or_high_top3_rows, 
                self.low_or_high_counter_df[self.low_or_high_counter_df['count'] == value]])
            
            if low_or_high_top3_rows.shape[0] >= 3:
                break

        if full_results:
            high_top_3_rows.to_csv(os.path.join('output', prefix + 'top_intervals_market_high.csv'), index=False)
            low_top_3_rows.to_csv(os.path.join('output', prefix + 'top_intervals_market_low.csv'), index=False)
        
        low_or_high_top3_rows.to_csv(os.path.join('output', prefix + 'top_intervals_market_low_or_high.csv'), index=False)
