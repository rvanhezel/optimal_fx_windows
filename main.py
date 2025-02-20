from src.logger import Logger
import logging
from dotenv import load_dotenv
from src.configuration import Configuration
from src.fx_time_interval_scanner import FxTimeIntervalScanner
from src.market_data_service import MarketDataService
import os


if __name__ == '__main__':
    # Load API keys
    load_dotenv()

    Logger()
    cfg = Configuration('run.cfg')
    market_data_service = MarketDataService(
        cfg,
        int(os.environ.get('MT5_LOGIN', 'WRONG-KEY')),
        os.environ.get('MT5_PASSWORD', 'WRONG-KEY'),
        os.environ.get('MT5_SERVER', 'WRONG-KEY')
    )
                                            

    for fx_rate in cfg.fx_rates:
        logging.info(f"Starting run for {fx_rate}")

        try:

            scanner = FxTimeIntervalScanner(
                cfg.tick_interval,
                fx_rate,
                cfg.low_high_interval,
                cfg.historical_data_horizon,
                cfg.historical_data_filename,
                market_data_service,
                cfg.spread,
                cfg.market_data_timezone,
                cfg.market_open_time)
            scanner.run_scanner()
            scanner.export_results(cfg.full_results)

        except Exception as err:
            logging.error(f'Error running {fx_rate}: {err}')
            raise

    market_data_service.close()
    logging.info(f"Exiting run")
