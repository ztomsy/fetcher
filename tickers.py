from tkgcore import Bot
from tkgcore import Tickers

import time
import sys

fetcher = Bot("_config_default.json", "fetcher.log")
fetcher.throttle = None

fetcher.get_cli_parameters(sys.argv[1:])
fetcher.load_config_from_file(fetcher.config_filename)

fetcher.noauth = True  # we are not going to authorize on exchange
fetcher.init_exchange()

if fetcher.throttle["enabled"]:
    fetcher.log(fetcher.LOG_INFO, "Enabling throttling:")
    fetcher.exchange.enable_requests_throttle(fetcher.throttle["lap_time"], fetcher.throttle["max_requests_per_lap"])

    fetcher.log(fetcher.LOG_INFO, "... lap_time (period)s: {} ".format(
        fetcher.exchange.requests_throttle.period))
    fetcher.log(fetcher.LOG_INFO, "... max_requests_per_lap: {} ".format(
        fetcher.exchange.requests_throttle.requests_per_period))

fetcher.log(fetcher.LOG_INFO, "Initializing DataBase... {}".format(fetcher.sqla["connection_string"]))
fetcher.init_remote_reports()
fetcher.log(fetcher.LOG_INFO, "... OK")

fetcher.sqla_reporter.TABLES = [Tickers]
created_tables = fetcher.sqla_reporter.create_tables()
if len(created_tables) > 0:
    fetcher.log(fetcher.LOG_INFO, "Created tables {}".format(created_tables))

fetch_num = 0
print("=========================================================================")

while True:
    sleep_time = fetcher.exchange.requests_throttle.sleep_time()

    time.sleep(sleep_time)
    try:
        tickers = fetcher.exchange.fetch_tickers()

    except Exception as e:
        fetcher.log(fetcher.LOG_ERROR, "Error fetching tickers:")
        fetcher.log(fetcher.LOG_ERROR, ".. exception: {}".format(type(e).__name__))
        fetcher.log(fetcher.LOG_ERROR, ".. exception body:", e.args)

    sys.stdout.write('\r')
    sys.stdout.write("Total Fetches: {total_fetches}. Current Period Time: {period_time}. "
                     "Requests in period: {requests_in_period} / {total_requests_in_period}. Sleep {sleep_time}".
                     format(total_fetches=fetch_num,
                            period_time=fetcher.exchange.requests_throttle._current_period_time,
                            requests_in_period=fetcher.exchange.requests_throttle.total_requests_current_period,
                            total_requests_in_period=fetcher.exchange.requests_throttle.requests_per_period,
                            sleep_time=sleep_time)
                     )

    sys.stdout.flush()
    fetch_num += 1

    # fetcher.sqla_reporter.new_session()
    try:
        fetcher.sqla_reporter.connection.execute(Tickers.__table__.insert(),
                                             Tickers.bulk_list_from_tickers(fetcher.exchange_id,
                                                                            tickers))
    except Exception as e:
        fetcher.log(fetcher.LOG_ERROR, "Error saving tickers:")
        fetcher.log(fetcher.LOG_ERROR, ".. exception: {}".format(type(e).__name__))
        fetcher.log(fetcher.LOG_ERROR, ".. exception body:", e.args)
