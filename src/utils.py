import pandas as pd
import holidays
import re


def create_overlapping_time_grid(start_date, end_date, tick_interval, horizon_length, time_only=True):
    start = start_date
    time_grid = []
    period_end = shift_date_by_period(horizon_length, start_date)
    while period_end <= end_date:
        time_grid.append((start, period_end))
        start = shift_date_by_period(tick_interval, start)
        period_end = shift_date_by_period(horizon_length, start)

    if time_only:
        return [(
        window[0].to_pydatetime().time(),
        window[1].to_pydatetime().time()
        ) for window in time_grid]
    else:
        return time_grid

def create_daily_date_schedule(start_date: pd.Timestamp, end_date: pd.Timestamp, check_bdays = True):
    new_date = start_date
    historical_period = [start_date]
    while new_date <= end_date:
        new_date += pd.Timedelta(days=1)

        if check_bdays:
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
    mask = (df.index >= start_time) & (df.index <= end_time)
    filtered_df = df[mask]

    max = filtered_df["high"].max()
    min = filtered_df["low"].min()
    return min, max

def market_open(date, market_calendar='NYSE'):
    if date.dayofweek > 4:
        return False

    # market_holidays = holidays.NYSE() if market_calendar == 'NYSE' else holidays.UnitedKingdom()  # Add more calendars as needed
    # if date.strftime('%Y-%m-%d') in market_holidays:
    #     return False

    return True

def split_tenor_string(entry):
    regex = re.compile("^(?P<numbers>\d*)(?P<letters>\w*)$")
    (numbers, letters) = regex.search(entry).groups()
    numbers = 1 if numbers == "" else numbers
    return (int(numbers), letters or None)


class Period: ...

def shift_date_by_period(period: Period, input_date: pd.Timestamp, direction: str = "+") -> pd.Timestamp:
    if direction not in ("+", "-"):
        raise ValueError("Operation to be performed not recognized")

    # Determine the number of units to adjust
    units = period.units if direction == "+" else -period.units

    # Adjust the input_date based on period.tenor
    if period.tenor.upper() == "MIN":  # mihnutes
        input_date += pd.Timedelta(minutes=units)
    elif period.tenor.upper() == "W":  # Weeks
        input_date += pd.Timedelta(weeks=units)
    elif period.tenor.upper() == "M":  # Months
        input_date += pd.offsets.DateOffset(months=units)
    elif period.tenor.upper() == "Q":  # Quarters
        input_date += pd.offsets.DateOffset(months=3 * units)
    elif period.tenor.upper() == "SA":  # Semi-Annual
        input_date += pd.offsets.DateOffset(months=6 * units)
    elif period.tenor.upper() == "Y":  # Years
        input_date += pd.offsets.DateOffset(years=units)
    elif period.tenor.upper() == "D":  # Days
        input_date += pd.Timedelta(days=units)
    elif period.tenor.upper() == "B":  # Business Days
        input_date += pd.offsets.BusinessDay(n=units)
    else:
        raise ValueError("Period tenor not recognized")

    return input_date
