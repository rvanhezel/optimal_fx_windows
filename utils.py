import pandas as pd
import holidays


def create_overlapping_time_grid(start_date, end_date, tick_interval, horizon_length, time_only=True):
    start = start_date
    time_grid = []
    period_end = start_date + pd.Timedelta(minutes=horizon_length)
    while period_end <= end_date:
        time_grid.append((start, period_end))
        start += pd.Timedelta(minutes=tick_interval)
        period_end = start + pd.Timedelta(minutes=horizon_length)

    if time_only:
        return [(
        window[0].to_pydatetime().time(),
        window[1].to_pydatetime().time()
        ) for window in time_grid]
    else:
        return time_grid

def create_daily_date_schedule(start_date: pd.Timestamp, end_date: pd.Timestamp, exclude_bdays = True):
    new_date = start_date
    historical_period = [start_date]
    while new_date <= end_date:
        new_date += pd.Timedelta(days=1)

        if exclude_bdays:
            if market_open(new_date):
                historical_period.append(new_date)
        else:
            historical_period.append(new_date)

    return historical_period

def count_points_between_dates(start_date: pd.Timestamp, later_date: pd.Timestamp, interval: str):
    data_points = (later_date - start_date).total_seconds() // pd.Timedelta(interval).total_seconds()
    return int(data_points) + 1

def high_low_per_window(window, df):
    start_time = window[0]   
    end_time = window[1]   
    mask = (df.index >= start_time) & (df.index < end_time)
    filtered_df = df[mask]

    max = filtered_df["high"].max()
    min = filtered_df["low"].min()
    return min, max

def market_open(date, market_calendar='NYSE'):
    if date.dayofweek > 4:
        return False

    market_holidays = holidays.NYSE() if market_calendar == 'NYSE' else holidays.UnitedKingdom()  # Add more calendars as needed
    if date.strftime('%Y-%m-%d') in market_holidays:
        return False

    return True