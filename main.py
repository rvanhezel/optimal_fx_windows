from src.logger import Logger
import logging
from dotenv import load_dotenv
from src.configuration import Configuration
from src.fx_time_interval_scanner import FxTimeIntervalScanner



if __name__ == '__main__':
    # Load API keys
    load_dotenv()

    Logger()
    cfg = Configuration('run.cfg')

    for fx_rate in cfg.fx_rates:
        logging.info(f"Starting run for {fx_rate}")

        try:

            scanner = FxTimeIntervalScanner(
                cfg.tick_interval,
                fx_rate,
                cfg.low_high_interval,
                cfg.historical_data_horizon)

            scanner.run_scanner()
            scanner.export_results()

        except Exception as err:
            logging.error(f'Error running {fx_rate}: {err}')

    logging.info(f"Exiting run")
