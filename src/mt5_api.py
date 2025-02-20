import MetaTrader5 as mt5
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional
import logging
from src.period import Period


class MT5API:
    """A wrapper for the MetaTrader 5 API with structured login and data retrieval methods."""

    def __init__(self, login: int, password: str, server: str) -> None:
        """
        Initializes and logs into MetaTrader 5.

        :param login: Trading account login ID.
        :param password: Trading account password.
        :param server: Broker server name.
        """
        if not mt5.initialize():
            msg = "Failed to initialize MetaTrader 5"
            logging.error(msg)
            raise ConnectionError(msg)

        if not mt5.login(login=login, password=password, server=server):
            error_code = mt5.last_error()
            mt5.shutdown()

            msg = f"Login failed. Error: {error_code}"
            logging.error(msg)
            raise ConnectionError(msg)

        logging.debug(f"Successfully logged into MT5 account: {login}")

    def copy_rates_range(
        self, symbol: str, 
        timeframe: Period, 
        start: datetime, 
        end: datetime
    ) -> Optional[pd.DataFrame]:
        """
        Retrieves historical rates for a symbol within a specified date range.

        :param symbol: Trading instrument (e.g., "EURUSD").
        :param timeframe: Timeframe constant from MT5 (e.g., mt5.TIMEFRAME_D1).
        :param start: Start datetime for historical data.
        :param end: End datetime for historical data.
        :return: A DataFrame containing historical rates or None if an error occurs.
        """
        mt5_timeframe = self._period_to_mt5_timeframe(timeframe)

        # There seems to be a max date range in the MT5 API. We subdivide the
        # date range in 6month intervals and make multiple calls instead of one large
        # call to handle this.
        six_months = timedelta(days=180)  
        current_start = start
        all_data = []

        while current_start < end:
            current_end = min(current_start + six_months, end)

            # Fetch data for the current range
            rates = mt5.copy_rates_range(symbol, mt5_timeframe, current_start, current_end)

            if rates is None:
                msg = f"Warning: No data retrieved for {symbol} from "
                msg += "{current_start} to {current_end}."
                logging.warning(msg)
                return None
            else:
                df = pd.DataFrame(rates)
                all_data.append(df)
            
            current_start = current_end

        # Convert to DataFrame
        full_df = pd.concat(all_data).drop_duplicates().reset_index(drop=True)
        full_df.index = pd.to_datetime(full_df["time"], unit="s")
        full_df.pop('time')
        full_df.sort_index(ascending=False, inplace=True)
        return full_df

    def close(self) -> None:
        """Closes the connection to MetaTrader 5."""
        mt5.shutdown()
        logging.debug("MT5 connection closed")

    def _period_to_mt5_timeframe(self, period: Period) -> int:
        """
        Converts a Period object (e.g., "5min") into an MT5 timeframe constant.

        :param period: Period object.
        :return: Corresponding MT5 timeframe constant.
        :raises ValueError: If the period unit is unsupported.
        """
        unit_mapping = {
            "min": {
                1: mt5.TIMEFRAME_M1,
                2: mt5.TIMEFRAME_M2,
                3: mt5.TIMEFRAME_M3,
                4: mt5.TIMEFRAME_M4,
                5: mt5.TIMEFRAME_M5,
                10: mt5.TIMEFRAME_M10,
                15: mt5.TIMEFRAME_M15,
                30: mt5.TIMEFRAME_M30,
            },
            "h": {
                1: mt5.TIMEFRAME_H1,
                2: mt5.TIMEFRAME_H2,
                3: mt5.TIMEFRAME_H3,
                4: mt5.TIMEFRAME_H4,
                6: mt5.TIMEFRAME_H6,
                8: mt5.TIMEFRAME_H8,
                12: mt5.TIMEFRAME_H12,
            },
            "d": {
                1: mt5.TIMEFRAME_D1,
            },
            "w": {
                1: mt5.TIMEFRAME_W1,
            },
            "mn": {
                1: mt5.TIMEFRAME_MN1,
            },
        }

        for unit, mapping in unit_mapping.items():
            if period.tenor.lower() == unit:
                mt5_timeframe = mapping.get(period.units, None)

        if mt5_timeframe is None:
            msg = f"MT5 unsupported timeframe"
            logging.error(msg)
            raise ValueError(msg)
        else:
            return mt5_timeframe
        
        