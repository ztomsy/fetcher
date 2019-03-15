from tkgcore import Bot
from tkgcore import Tickers

import sys
from tkgcore import ccxtExchangeWrapper

fetcher = Bot("_config_default.json", "fetcher.log")
fetcher.get_cli_parameters(sys.argv[1:])
fetcher.load_config_from_file(fetcher.config_filename)

fetcher.noauth = True  # we are not going to authorize on exchange
fetcher.init_exchange()

tickers = fetcher.exchange.fetch_tickers()

fetcher.init_remote_reports()
fetcher.sqla_reporter.TABLES = [Tickers]
fetcher.sqla_reporter.create_tables()

# adding single ticker
fetcher.sqla_reporter.new_session()
fetcher.sqla_reporter.session.add(Tickers.from_single_ticker("ETH/BTC", tickers["ETH/BTC"]))
fetcher.sqla_reporter.session.commit()

# adding list of tickers
fetcher.sqla_reporter.connection.execute(Tickers.__table__.insert(), Tickers.bulk_list_from_tickers(tickers))

