from ztom import Bot
from ztom import Tickers
from ztom import timer
import time
import sys

fetcher = Bot("_config_default.json", "fetcher.log")

# init throttle field to be read from the config
fetcher.throttle = None

fetcher.set_from_cli(sys.argv[1:])
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

if fetcher.sqla is not None and fetcher.sqla["enabled"]:
    fetcher.sqla_reporter.TABLES = [Tickers]
    created_tables = fetcher.sqla_reporter.create_tables()
    if len(created_tables) > 0:
        fetcher.log(fetcher.LOG_INFO, "Created tables {}".format(created_tables))
else:
    fetcher.log(fetcher.LOG_INFO, "Will not save to DB")

fetch_num = 0
saved_to_db = False
print("=========================================================================")

timer = timer.Timer()
timer_report = dict()

while True:
    sleep_time = fetcher.exchange.requests_throttle.sleep_time()
    # will
    time.sleep(sleep_time)
    try:
        timer.reset_notches()
        timer.notch("start")
        tickers = fetcher.exchange.fetch_tickers()

    except Exception as e:
        fetcher.log(fetcher.LOG_ERROR, "Error fetching tickers:")
        fetcher.log(fetcher.LOG_ERROR, ".. exception: {}".format(type(e).__name__))
        fetcher.log(fetcher.LOG_ERROR, ".. exception body:", e.args)
    timer.notch("tickers_received")

    tps = fetcher.exchange.requests_throttle.total_requests_current_period / fetcher.exchange.requests_throttle._current_period_time \
        if fetcher.exchange.requests_throttle._current_period_time != 0 else 0.0

    sys.stdout.write('\r')
    sys.stdout.write("Total Fetches: {total_fetches}. Fetch time: {fetch_time}. db_time: {db_write_time} "
                     "TPS current period: "
                     "{tps}. Current Period Time: {period_time}. "
                     "Requests in period: {requests_in_period} / {total_requests_in_period}. Sleep {sleep_time}. "
                     "Saved to DB: {saved_to_db}".
                     format(total_fetches=fetch_num,
                            tps=tps,
                            period_time=fetcher.exchange.requests_throttle._current_period_time,
                            requests_in_period=fetcher.exchange.requests_throttle.total_requests_current_period,
                            total_requests_in_period=fetcher.exchange.requests_throttle.requests_per_period,
                            sleep_time=sleep_time,
                            saved_to_db=saved_to_db,
                            fetch_time=timer_report.get("tickers_received"),
                            db_write_time=timer_report.get("db_write")
                            )
                     )

    sys.stdout.flush()
    fetch_num += 1
    saved_to_db = False
    timer.notch("output")
    if fetcher.sqla["enabled"]:
        try:
            # fast bulk insert as referenced:
            # https://stackoverflow.com/questions/45484171/sqlalchemy-bulk-insert-is-slower-than-building-raw-sql

            fetcher.sqla_reporter.connection.execute(Tickers.__table__.insert().values(
                                                 Tickers.bulk_list_from_tickers(fetcher.exchange_id,
                                                                                tickers)))

            saved_to_db = True
        except Exception as e:
            fetcher.log(fetcher.LOG_ERROR, "Error saving tickers:")
            fetcher.log(fetcher.LOG_ERROR, ".. exception: {}".format(type(e).__name__))
            fetcher.log(fetcher.LOG_ERROR, ".. exception body:", e.args)
        timer.notch("db_write")

        timer_report = timer.results_dict()
