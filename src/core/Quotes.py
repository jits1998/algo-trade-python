import logging
import statistics
import time
from datetime import datetime, timedelta, timezone

import pandas as pd
import pandas_ta as ta
import requests
from kiteconnect.exceptions import DataException, NetworkException
from requests.exceptions import ReadTimeout

from core.Controller import Controller
from instruments.Instruments import Instruments
from models.Quote import Quote
from utils.Utils import Utils


class Quotes:
    def __init__(self, candles_table="historical_candles"):
        """Initialize Quotes"""
        self.candles_table = candles_table
        self._trading_day_cache = {}  # date_str -> bool

    def _getFromQuestDB(self, tradingSymbol, short_code, date_str, from_time=None, to_time=None):
        """
        Retrieve candles from QuestDB cache.
        Validates that cached data covers the full requested range by checking
        boundary timestamps. Returns None on partial data so the caller
        falls through to the broker API (which will replace the partial data).

        Returns list of candles or None if not found/partial/no cursor.
        """
        try:
            actual_from = from_time or "09:15:00"
            actual_to = to_time or "15:30:00"
            from_dt = f"{date_str} {actual_from}"
            to_dt = f"{date_str} {actual_to}"

            rows = None
            with Utils._getQuestDBCursor() as cursor:
                cursor.execute(
                    f"""
          SELECT ts, open, high, low, close, volume, oi FROM {self.candles_table}
          WHERE trading_symbol = %s
            AND ts >= %s::timestamp AND ts <= %s::timestamp
          ORDER BY ts
        """,
                    (tradingSymbol, from_dt, to_dt),
                )

                rows = cursor.fetchall()

            if rows and len(rows) > 0:
                candles = []
                for row in rows:
                    # Skip empty rows
                    if not row or len(row) < 6:
                        logging.debug(f"Skipping invalid/empty row for {tradingSymbol}: {row}")
                        continue

                    try:
                        ts = row[0]
                        # Strip timezone info - keep timestamps as naive datetime objects
                        if hasattr(ts, "tzinfo") and ts.tzinfo:
                            ts = ts.replace(tzinfo=None)

                        candle = {
                            "date": ts,
                            "open": row[1],
                            "high": row[2],
                            "low": row[3],
                            "close": row[4],
                            "volume": row[5],
                            "oi": row[6] if len(row) > 6 else 0,
                        }
                        candles.append(candle)
                    except (IndexError, TypeError) as e:
                        logging.error(
                            f"Error parsing QuestDB row for {tradingSymbol}: {e}. Row data: {row}"
                        )
                        continue

                # Validate boundary timestamps to detect partial data
                # Skip validation for single candle requests (from == to)
                if actual_from != actual_to:
                    first_ts = candles[0]["date"]
                    last_ts = candles[-1]["date"]

                    # Construct datetime objects for requested range
                    from_datetime = datetime.strptime(
                        f"{date_str} {actual_from}", "%Y-%m-%d %H:%M:%S"
                    )
                    to_datetime = datetime.strptime(f"{date_str} {actual_to}", "%Y-%m-%d %H:%M:%S")

                    # Strip timezone and seconds for minute-level comparison
                    def to_minute(dt):
                        if hasattr(dt, "tzinfo") and dt.tzinfo:
                            dt = dt.replace(tzinfo=None)
                        return dt.replace(second=0, microsecond=0)

                    first_min = to_minute(first_ts)
                    last_min = to_minute(last_ts)
                    from_min = to_minute(from_datetime)
                    to_min = to_minute(to_datetime)

                    # Exact match on start boundary
                    if first_min != from_min:
                        logging.debug(
                            f"QuestDB partial data for {tradingSymbol}: first candle at {first_ts}, expected {actual_from}. Treating as cache miss."
                        )
                        return None

                    # End boundary: accept up to 1 minute before requested end
                    # (broker APIs typically return last candle at 15:29, not 15:30)
                    if last_min < to_min - timedelta(minutes=1):
                        logging.debug(
                            f"QuestDB partial data for {tradingSymbol}: last candle at {last_ts}, expected {actual_to}. Treating as cache miss."
                        )
                        return None

                logging.debug(f"Cache hit in QuestDB: {len(candles)} candles for {tradingSymbol}")
                return candles
        except Exception as e:
            import traceback

            logging.warning(f"Error querying QuestDB cache for {tradingSymbol}: {str(e)}")
            logging.debug(f"Full traceback: {traceback.format_exc()}")

        return None

    def _isTradingDay(self, check_date, short_code):
        """
        Check if a given date had trading by looking for NIFTY 50 quote.
        Uses QuestDB cache if available, otherwise tries to fetch a single candle.

        Args:
            check_date: datetime object to check
            short_code: Broker short code

        Returns:
            True if trading day (NIFTY 50 data exists), False otherwise
        """
        def _cache(value):
            self._trading_day_cache[date_str] = value
            return self._trading_day_cache[date_str]

        try:
            date_str = check_date.strftime("%Y-%m-%d")

            if date_str in self._trading_day_cache:
                return self._trading_day_cache[date_str]

            # Check if NIFTY 50 has data for this date in QuestDB
            try:
                result = None
                with Utils._getQuestDBCursor() as cursor:
                    cursor.execute(
                        f"""
            SELECT COUNT(*) FROM {self.candles_table}
            WHERE trading_symbol = %s AND ts >= %s::timestamp AND ts < %s::timestamp
            LIMIT 1
          """,
                        ("NIFTY26APRFUT", f"{date_str} 09:15:00", f"{date_str} 15:31:00"),
                    )

                    result = cursor.fetchone()
                if result and result[0] > 0:
                    logging.debug(f"Trading day confirmed for {date_str} (found NIFTY 50 data)")
                    return _cache(True)
            except Exception as e:
                logging.debug(f"Error checking QuestDB for trading day: {str(e)}")

            # If not in QuestDB, try to fetch a single candle from API
            try:
                candles = self.getHistoricalData(
                    tradingSymbol="NIFTY 50",
                    short_code=short_code,
                    date_str=date_str,
                    isFnO=False,
                    exchange="NSE",
                    from_time="09:15:00",
                    to_time="09:16:00",
                )
                if candles and len(candles) > 0:
                    logging.debug(f"Trading day confirmed for {date_str} (found NIFTY 50 quote)")
                    return _cache(True)
            except Exception as e:
                logging.debug(f"Error fetching NIFTY 50 for {date_str}: {str(e)}")

            logging.debug(f"No trading on {date_str} (NIFTY 50 data not found)")
            return _cache(False)

        except Exception as e:
            logging.warning(f"Error checking if {check_date} is trading day: {str(e)}")
            return False

    def _storeInQuestDB(self, tradingSymbol, short_code, candles):
        """
        Store candles in QuestDB for future caching.
        Uses upsert logic: checks existing timestamps and only inserts
        candles that don't already exist, avoiding duplicates without deleting.
        """
        if not candles:
            return

        # With DEDUP enabled, QuestDB automatically handles duplicates
        # No need for manual SELECT/check - just insert all candles
        try:
            inserted_count = 0
            with Utils._getQuestDBCursor() as cursor:
                for candle in candles:
                    candle_date = candle["date"]

                    # Strip any timezone info
                    if candle_date.tzinfo:
                        candle_date = candle_date.replace(tzinfo=None)

                    # Skip candles with non-:00 second timestamps (Zerodha first-tick artifacts)
                    if candle_date.second != 0:
                        logging.debug(
                            f"Skipping candle with non-:00 timestamp {candle_date} for {tradingSymbol}"
                        )
                        continue

                    ts_str = candle_date.strftime("%Y-%m-%d %H:%M:%S")

                    cursor.execute(
                        f"""
            INSERT INTO {self.candles_table}
            (ts, trading_symbol, open, high, low, close, volume, oi)
            VALUES (%s::timestamp, %s, %s, %s, %s, %s, %s, %s)
          """,
                        (
                            ts_str,
                            tradingSymbol,
                            candle["open"],
                            candle["high"],
                            candle["low"],
                            candle["close"],
                            candle["volume"],
                            candle.get("oi", 0) or 0,
                        ),
                    )
                    inserted_count += 1

            # Log results (autocommit handles the commit, DEDUP handles duplicates)
            logging.debug(
                f"Inserted {inserted_count} candles for {tradingSymbol} in QuestDB (DEDUP will handle any duplicates)"
            )
        except Exception as e:
            logging.warning(f"Error storing candles in QuestDB: {str(e)}")

    @staticmethod
    def getQuote(tradingSymbol, short_code, isFnO=False, exchange="NFO"):
        broker = Controller.getBrokerName(short_code)
        brokerHandle = Controller.getBrokerLogin(short_code).getBrokerHandle()
        quote = None
        if broker == "zerodha":
            key = (
                (exchange + ":" + tradingSymbol.upper())
                if isFnO == True
                else ("NSE:" + tradingSymbol.upper())
            )
            quote = Quote(tradingSymbol)

            bQuoteResp = Quotes._getQuote(brokerHandle, key)

            bQuote = bQuoteResp[key]

            # convert broker quote to our system quote
            quote.tradingSymbol = tradingSymbol
            quote.lastTradedPrice = bQuote["last_price"]
            quote.lastTradedQuantity = bQuote["last_quantity"]
            quote.avgTradedPrice = bQuote["average_price"]
            quote.volume = bQuote["volume"]
            quote.totalBuyQuantity = bQuote["buy_quantity"]
            quote.totalSellQuantity = bQuote["sell_quantity"]
            ohlc = bQuote["ohlc"]
            quote.open = ohlc["open"]
            quote.high = ohlc["high"]
            quote.low = ohlc["low"]
            quote.close = ohlc["close"]
            quote.change = bQuote["net_change"]
            quote.oiDayHigh = bQuote["oi_day_high"]
            quote.oiDayLow = bQuote["oi_day_low"]
            quote.oi = bQuote["oi"]
            quote.lowerCiruitLimit = bQuote["lower_circuit_limit"]
            quote.upperCircuitLimit = bQuote["upper_circuit_limit"]
        elif broker == "icici":
            isd = Instruments.getInstrumentDataBySymbol(short_code, tradingSymbol)
            bQuote = Quotes._getQuote(brokerHandle, isd)
            quote = Quote(tradingSymbol)
            quote.tradingSymbol = tradingSymbol
            quote.lastTradedPrice = bQuote["ltp"]
            quote.lastTradedQuantity = 0
            quote.avgTradedPrice = 0
            quote.volume = bQuote["total_quantity_traded"]
            quote.totalBuyQuantity = 0
            quote.totalSellQuantity = 0
            quote.open = bQuote["open"]
            quote.high = bQuote["high"]
            quote.low = bQuote["low"]
            quote.close = bQuote["previous_close"]
            quote.change = 0
            quote.oiDayHigh = 0
            quote.oiDayLow = 0
            quote.oi = 0
            quote.lowerCiruitLimit = bQuote["lower_circuit"]
            quote.upperCircuitLimit = bQuote["upper_circuit"]

        else:
            # The logic may be different for other brokers
            quote = None
        return quote

    @staticmethod
    def getCMP(tradingSymbol, short_code):
        quote = Quotes.getQuote(tradingSymbol, short_code)
        if quote:
            return quote.lastTradedPrice
        else:
            return 0

    @staticmethod
    def getIndexQuote(tradingSymbol, short_code, exchange="NSE"):
        broker = Controller.getBrokerName(short_code)
        brokerHandle = Controller.getBrokerLogin(short_code).getBrokerHandle()
        quote = None
        if broker == "zerodha":
            key = exchange + ":" + tradingSymbol.upper()

            bQuoteResp = Quotes._getQuote(brokerHandle, key)

            bQuote = bQuoteResp[key]
            # convert broker quote to our system quote
            quote = Quote(tradingSymbol)
            quote.tradingSymbol = tradingSymbol
            quote.lastTradedPrice = bQuote["last_price"]
            ohlc = bQuote["ohlc"]
            quote.open = ohlc["open"]
            quote.high = ohlc["high"]
            quote.low = ohlc["low"]
            quote.close = ohlc["close"]
            quote.change = bQuote["net_change"]
        elif broker == "icici":
            isd = Instruments.getInstrumentDataBySymbol(short_code, tradingSymbol)
            bQuote = Quotes._getQuote(brokerHandle, isd)
            quote = Quote(tradingSymbol)
            quote.tradingSymbol = tradingSymbol
            quote.lastTradedPrice = bQuote["ltp"]
            quote.lastTradedQuantity = 0
            quote.avgTradedPrice = 0
            quote.volume = bQuote["total_quantity_traded"]
            quote.totalBuyQuantity = 0
            quote.totalSellQuantity = 0
            quote.open = bQuote["open"]
            quote.high = bQuote["high"]
            quote.low = bQuote["low"]
            quote.close = bQuote["previous_close"]
            quote.change = 0
            quote.oiDayHigh = 0
            quote.oiDayLow = 0
            quote.oi = 0
            quote.lowerCiruitLimit = bQuote["lower_circuit"]
            quote.upperCircuitLimit = bQuote["upper_circuit"]
        else:
            # The logic may be different for other brokers
            quote = None
        return quote

    @staticmethod
    def _getQuote(brokerHandle, key):
        retry = True
        bQuoteResp = None

        while retry:
            retry = False
            try:
                bQuoteResp = brokerHandle.quote(key)
            except DataException as de:
                if de.code in [503, 502]:
                    retry = True
            except requests.HTTPError as he:
                if he.response.status_code in [503, 502]:
                    retry = True
            except NetworkException as ne:
                if ne.code in [429]:
                    time.sleep(1)  # extra 1 sec wait for too many requests
                    retry = True
            except ValueError:
                retry = True
            except ReadTimeout:
                retry = True
            if retry:
                time.sleep(1)
                logging.info("retrying getQuote after 1 s for %s", key)
        return bQuoteResp

    def getHistoricalData(
        self,
        tradingSymbol,
        short_code,
        date_str,
        isFnO=False,
        exchange="NFO",
        from_time=None,
        to_time=None,
        underlying=None,
        expiry_date=None,
    ):
        """
        Get historical candle data for a trading symbol for a specific date.
        Always returns 1-minute candles for both Zerodha and ICICI.
        Data is cached in QuestDB (if available) and in-memory to avoid redundant API calls.

        Args:
            tradingSymbol: Trading symbol to get historical data for
            short_code: Broker short code
            date_str: Date string in YYYY-MM-DD format
            isFnO: Whether it's F&O instrument
            exchange: Exchange name (NFO for F&O, NSE for equity)
            from_time: Optional start time in HH:MM:SS format (default: 09:15:00 for full day)
            to_time: Optional end time in HH:MM:SS format (default: 15:30:00 for full day)
            underlying: Optional underlying symbol name (e.g., 'NIFTY', 'BANKNIFTY') - for ICICI when instrument not found
            expiry_date: Optional expiry date in YYYY-MM-DD format - for ICICI when instrument not found

        Returns:
            list: List of candle dictionaries with keys: date, open, high, low, close, volume
                  Returns None on error
        """
        logging.debug(
            "getHistoricalData: symbol=%s short_code=%s date=%s from=%s to=%s isFnO=%s exchange=%s underlying=%s expiry=%s",
            tradingSymbol, short_code, date_str, from_time, to_time, isFnO, exchange, underlying, expiry_date,
        )

        # Check QuestDB cache first (broker-agnostic)
        cached_from_questdb = self._getFromQuestDB(
            tradingSymbol, short_code, date_str, from_time, to_time
        )

        broker = Controller.getBrokerName(short_code)
        brokerHandle = Controller.getBrokerLogin(short_code).getBrokerHandle()

        if broker == "icici":
            if cached_from_questdb:
                logging.debug("getHistoricalData: QuestDB cache hit for %s — %d candles", tradingSymbol, len(cached_from_questdb))
                return cached_from_questdb
            result = self._getHistoricalDataICICI(
                tradingSymbol,
                short_code,
                brokerHandle,
                date_str,
                isFnO,
                exchange,
                from_time,
                to_time,
                underlying,
                expiry_date,
            )
            logging.debug("getHistoricalData: broker(icici) returned %s candles for %s", len(result) if result else 0, tradingSymbol)
            return result
        elif broker != "zerodha":
            logging.warning("getHistoricalData currently only supports Zerodha and ICICI brokers")
            return None

        if cached_from_questdb:
            logging.debug("getHistoricalData: QuestDB cache hit for %s — %d candles", tradingSymbol, len(cached_from_questdb))
            return cached_from_questdb

        try:
            # Get instrument token
            isd = Instruments.getInstrumentDataBySymbol(short_code, tradingSymbol)
            if not isd:
                logging.error("Instrument data not found for %s", tradingSymbol)
                return None

            instrument_token = isd["instrument_token"]

            # Parse the date string and set time range
            date_obj = datetime.strptime(date_str, "%Y-%m-%d")

            # Use custom time range if provided, otherwise default to market hours
            if from_time:
                # Parse HH:MM:SS format
                time_parts = from_time.split(":")
                from_date = date_obj.replace(
                    hour=int(time_parts[0]),
                    minute=int(time_parts[1]),
                    second=int(time_parts[2]) if len(time_parts) > 2 else 0,
                    microsecond=0,
                )
            else:
                # Default: 9:15 AM
                from_date = date_obj.replace(hour=9, minute=15, second=0, microsecond=0)

            if to_time:
                # Parse HH:MM:SS format
                time_parts = to_time.split(":")
                to_date = date_obj.replace(
                    hour=int(time_parts[0]),
                    minute=int(time_parts[1]),
                    second=int(time_parts[2]) if len(time_parts) > 2 else 0,
                    microsecond=0,
                )
            else:
                # Default: 3:30 PM
                to_date = date_obj.replace(hour=15, minute=30, second=0, microsecond=0)

            # Format dates as required by API: yyyy-mm-dd HH:MM:SS
            # Always use :00 for from and :01 for to so Zerodha returns :00-aligned candles
            from_str = from_date.replace(second=0).strftime("%Y-%m-%d %H:%M:%S")
            to_str = to_date.replace(second=1).strftime("%Y-%m-%d %H:%M:%S")

            logging.debug(
                f"Fetching historical data for {tradingSymbol} from {from_str} to {to_str}"
            )

            # Fetch historical data with retry logic (always use 1-minute interval)
            retry = True
            historical_data = None

            while retry:
                retry = False
                try:
                    historical_data = brokerHandle.historical_data(
                        instrument_token=instrument_token,
                        from_date=from_str,
                        to_date=to_str,
                        interval="minute",
                        oi=True,
                    )
                except DataException as de:
                    if de.code in [503, 502]:
                        retry = True
                except requests.HTTPError as he:
                    if he.response.status_code in [503, 502]:
                        retry = True
                except NetworkException as ne:
                    if ne.code in [429]:
                        logging.warning(f"Rate limit hit for {tradingSymbol}, waiting 0.35 second")
                        time.sleep(0.35)
                        retry = True
                except ValueError:
                    retry = True
                except ReadTimeout:
                    retry = True

                if retry:
                    time.sleep(1)
                    logging.info("Retrying historical_data after 1s for %s", tradingSymbol)

            if not historical_data or len(historical_data) == 0:
                logging.warning(f"No historical data returned for {tradingSymbol} on {date_str}")
                return None

            logging.debug(
                f"Fetched {len(historical_data)} candles for {tradingSymbol} on {date_str}"
            )

            # Strip timezone info from all candles
            for candle in historical_data:
                if "date" in candle and hasattr(candle["date"], "tzinfo") and candle["date"].tzinfo:
                    candle["date"] = candle["date"].replace(tzinfo=None)

            # Store fetched data in QuestDB for future caching
            self._storeInQuestDB(tradingSymbol, short_code, historical_data)

            return historical_data

        except Exception as e:
            logging.error(f"Error in getHistoricalData for {tradingSymbol} on {date_str}: {str(e)}")
            return None

    def _getHistoricalDataICICI(
        self,
        tradingSymbol,
        short_code,
        brokerHandle,
        date_str,
        isFnO=False,
        exchange="NFO",
        from_time=None,
        to_time=None,
        underlying=None,
        expiry_date=None,
    ):
        """
        Get historical candle data for ICICI Breeze API.
        Always fetches 1-minute candles.

        Args:
            tradingSymbol: Trading symbol to get historical data for
            short_code: Broker short code
            brokerHandle: ICICI Breeze broker handle
            date_str: Date string in YYYY-MM-DD format
            isFnO: Whether it's F&O instrument
            exchange: Exchange name (NFO for F&O, NSE for equity)
            from_time: Optional start time in HH:MM:SS format (default: 09:15:00 for full day)
            to_time: Optional end time in HH:MM:SS format (default: 15:30:00 for full day)
            underlying: Optional underlying symbol name (e.g., 'NIFTY', 'BANKNIFTY')
            expiry_date: Optional expiry date in YYYY-MM-DD format

        Returns:
            list: List of candle dictionaries with keys: date, open, high, low, close, volume
        """
        try:
            from utils.Utils import Utils

            # Get instrument data
            isd = Instruments.getInstrumentDataBySymbol(short_code, tradingSymbol)

            # If instrument not found (e.g., historical options), create from provided data
            if not isd.get("name"):
                logging.debug(
                    f"Instrument data not found for {tradingSymbol}, using provided underlying and expiry"
                )

                # Use Utils methods to extract what we can
                option_type = Utils.getTypeFromSymbol(tradingSymbol)  # CE or PE

                if option_type in ["CE", "PE"]:
                    strike = Utils.getStrikeFromSymbol(tradingSymbol)

                    # Check if underlying and expiry were provided
                    if not underlying or not expiry_date:
                        logging.error(
                            f"Instrument not found for {tradingSymbol} and underlying/expiry not provided"
                        )
                        return None

                    # Create instrument data from provided info
                    isd = {
                        "name": brokerHandle.getStockCode(underlying),
                        "exchange": exchange,
                        "instrument_type": option_type,
                        "expiry": expiry_date,  # Already in YYYY-MM-DD format
                        "strike": str(strike),
                    }
                    logging.debug(f"Created instrument data from symbol: {isd}")
                elif brokerHandle.getStockCode(tradingSymbol) != tradingSymbol:
                    isd = {
                        "name": brokerHandle.getStockCode(tradingSymbol),
                        "exchange": "BSE",
                        "instrument_type": "EQ",
                        "segment": "INDICES",
                    }
                else:
                    logging.error(f"Could not parse instrument data from {tradingSymbol}")
                    return None

            # Parse the date string and set time range
            date_obj = datetime.strptime(date_str, "%Y-%m-%d")

            # Use custom time range if provided, otherwise default to market hours
            if from_time:
                time_parts = from_time.split(":")
                from_date = date_obj.replace(
                    hour=int(time_parts[0]),
                    minute=int(time_parts[1]),
                    second=int(time_parts[2]) if len(time_parts) > 2 else 0,
                    microsecond=0,
                )
            else:
                # Default: 9:15 AM
                from_date = date_obj.replace(hour=9, minute=15, second=0, microsecond=0)

            if to_time:
                time_parts = to_time.split(":")
                to_date = date_obj.replace(
                    hour=int(time_parts[0]),
                    minute=int(time_parts[1]),
                    second=int(time_parts[2]) if len(time_parts) > 2 else 0,
                    microsecond=0,
                )
            else:
                # Default: 3:30 PM
                to_date = date_obj.replace(hour=15, minute=30, second=0, microsecond=0)

            # Format dates as required by Breeze API
            # Format: YYYY-MM-DD HH:MM:SS
            from_str = from_date.strftime("%Y-%m-%d %H:%M:%S")
            to_str = to_date.strftime("%Y-%m-%d %H:%M:%S")

            logging.debug(
                f"Fetching ICICI historical data for {tradingSymbol} from {from_str} to {to_str}"
            )

            # Always use 1 minute candles for ICICI
            icici_interval = "1minute"

            # Determine product type and right for the API call
            product_type = "cash"
            right = "others"
            expiry_date = ""
            strike_price = ""

            # Check instrument type to determine product type
            instrument_type = isd.get("instrument_type", "")

            # Convert expiry date from DD-MMM-YYYY to YYYY-MM-DD format (if needed)
            # This is common for all F&O instruments
            expiry_str = isd.get("expiry", "")
            if expiry_str:
                # Try DD-MMM-YYYY format first (from instrument master)
                try:
                    expiry_obj = datetime.strptime(expiry_str, "%d-%b-%Y")
                    api_expiry_date = expiry_obj.strftime("%Y-%m-%d")
                except ValueError:
                    # Already in YYYY-MM-DD format (from provided parameter)
                    api_expiry_date = expiry_str
            else:
                api_expiry_date = ""

            if instrument_type == "PE":
                product_type = "options"
                right = "put"
                strike_price = str(isd.get("strike", ""))
            elif instrument_type == "CE":
                product_type = "options"
                right = "call"
                strike_price = str(isd.get("strike", ""))
            elif instrument_type == "FUT":
                product_type = "futures"
                right = "others"
            else:
                # For cash/equity/indices - product_type remains "cash"
                # api_expiry_date, right, and strike_price remain empty/default
                product_type = "cash"
                right = "others"
                api_expiry_date = ""
                strike_price = ""

            # Fetch historical data with retry logic
            retry = True
            historical_data = None
            retry_count = 0
            max_retries = 3

            while retry and retry_count < max_retries:
                retry = False
                retry_count += 1
                try:
                    # Call Breeze API's get_historical_data method
                    response = brokerHandle.get_historical_data(
                        interval=icici_interval,
                        from_date=from_str,
                        to_date=to_str,
                        stock_code=brokerHandle.getStockCode(isd["name"]),
                        exchange_code=exchange,
                        product_type=product_type,
                        expiry_date=api_expiry_date if api_expiry_date else "",
                        right=right,
                        strike_price=strike_price if strike_price else "",
                    )

                    if response and response.get("Status") == 200 and response.get("Success"):
                        historical_data = response["Success"]
                    else:
                        logging.error(
                            f"ICICI API error for {tradingSymbol}: {response} | "
                            f"params: interval={icici_interval}, from_date={from_str}, to_date={to_str}, "
                            f"stock_code={isd['name']}, exchange_code={exchange}, product_type={product_type}, "
                            f"expiry_date={api_expiry_date if api_expiry_date else ''}, right={right}, "
                            f"strike_price={strike_price if strike_price else ''}"
                        )
                        return None

                except requests.HTTPError as he:
                    if he.response.status_code in [503, 502, 429]:
                        retry = True
                        logging.warning(
                            f"HTTP error {he.response.status_code} for {tradingSymbol}, retrying..."
                        )
                except ValueError as ve:
                    logging.error(f"Value error in ICICI historical data: {ve}")
                    retry = True
                except ReadTimeout:
                    logging.warning(f"Read timeout for {tradingSymbol}, retrying...")
                    retry = True
                except Exception as e:
                    logging.error(f"Error fetching ICICI historical data: {e}")
                    return None

                if retry:
                    time.sleep(1)
                    logging.info(f"Retrying ICICI historical_data after 1s for {tradingSymbol}")

            # Add rate limiting for ICICI: 100 calls/minute = 600ms per call minimum
            time.sleep(0.6)

            if not historical_data or len(historical_data) == 0:
                logging.warning(
                    f"No historical data returned from ICICI for {tradingSymbol} on {date_str}"
                )
                return None

            # Convert ICICI format to standard format
            # ICICI returns: datetime, stock_code, exchange_code, open, high, low, close, volume, open_interest

            standardized_data = []
            for candle in historical_data:
                candle_datetime = (
                    datetime.strptime(candle["datetime"], "%Y-%m-%d %H:%M:%S")
                    if isinstance(candle["datetime"], str)
                    else candle["datetime"]
                )

                # Filter by time range if specified
                if from_time or to_time:
                    candle_time = candle_datetime.time()

                    if from_time:
                        from_time_obj = datetime.strptime(from_time, "%H:%M:%S").time()
                        if candle_time < from_time_obj:
                            continue

                    if to_time:
                        to_time_obj = datetime.strptime(to_time, "%H:%M:%S").time()
                        if candle_time > to_time_obj:
                            continue

                standardized_candle = {
                    "date": candle_datetime,
                    "open": float(candle["open"]),
                    "high": float(candle["high"]),
                    "low": float(candle["low"]),
                    "close": float(candle["close"]),
                    "volume": int(candle["volume"]),
                    "oi": int(candle.get("open_interest", 0) or 0),
                }
                standardized_data.append(standardized_candle)

            logging.debug(
                f"Fetched {len(standardized_data)} candles for {tradingSymbol} on {date_str}"
            )

            # Store fetched data in QuestDB for future caching
            self._storeInQuestDB(tradingSymbol, short_code, standardized_data)

            return standardized_data

        except Exception as e:
            logging.error(
                f"Error in _getHistoricalDataICICI for {tradingSymbol} on {date_str}: {str(e)}"
            )
            return None

    def calculateFetchRanges(
        self, calculation_time, num_candles, units_per_candle, unit_type="minutes", short_code=None
    ):
        """
        Calculate start and end time ranges to fetch required candles from historical API.
        Respects market hours (9:15 AM - 3:30 PM) and splits across days if needed.
        Includes partial last candle if calculation_time doesn't fall on a candle boundary.

        Args:
            calculation_time: The end point for ATR calculation (datetime object or string 'YYYY-MM-DD HH:MM:SS')
            num_candles: Number of candles needed for ATR calculation
            units_per_candle: How many time units form a single candle (e.g., 3 for 3-minute candles)
            unit_type: Type of time unit - 'minutes' or 'hours' (default: 'minutes')

        Returns:
            List of dicts: [{'date': 'YYYY-MM-DD', 'from_time': 'HH:MM:SS', 'to_time': 'HH:MM:SS'}, ...]
            Returns empty list on error

        Example:
            # Need 3 three-minute candles ending at 9:24 (includes partial candle 9:21-9:24)
            ranges = Quotes.calculateFetchRanges(datetime(2024, 3, 23, 9, 24), 3, 3, 'minutes')
            # Returns: [{'date': '2024-03-23', 'from_time': '09:15:00', 'to_time': '09:24:00'}]

            # Need 5 one-hour candles ending at 2:00 PM
            ranges = Quotes.calculateFetchRanges(datetime(2024, 3, 23, 14, 0), 5, 1, 'hours')
        """
        try:
            # Convert string to datetime if needed
            if isinstance(calculation_time, str):
                calculation_time = datetime.strptime(calculation_time, "%Y-%m-%d %H:%M:%S")

            # Convert unit_type to minutes for calculation
            if unit_type == "hours":
                candle_interval_minutes = units_per_candle * 60
            elif unit_type == "minutes":
                candle_interval_minutes = units_per_candle
            else:
                raise ValueError(f"Invalid unit_type: {unit_type}. Must be 'minutes' or 'hours'")

            # Calculate total minutes needed
            # Check if calculation_time falls on a candle boundary
            market_open = calculation_time.replace(hour=9, minute=15, second=0, microsecond=0)
            minutes_since_open = int((calculation_time - market_open).total_seconds() / 60)

            # Calculate how many minutes into the current candle we are
            partial_candle_minutes = 0
            if minutes_since_open >= 0:
                partial_candle_minutes = minutes_since_open % candle_interval_minutes

            if partial_candle_minutes > 0:
                # We have a partial candle at the end
                # Need (num_candles - 1) complete candles + partial candle
                total_minutes_needed = (
                    num_candles - 1
                ) * candle_interval_minutes + partial_candle_minutes
                logging.debug(
                    f"Partial candle detected: {partial_candle_minutes}/{candle_interval_minutes} minutes. Fetching {num_candles-1} complete + 1 partial = {num_candles} total candles"
                )
            else:
                # No partial candle, calculation_time falls on candle boundary
                # Need num_candles complete candles
                total_minutes_needed = num_candles * candle_interval_minutes
                logging.debug(f"No partial candle. Fetching {num_candles} complete candles")

            end_datetime = calculation_time

            # Market hours constants
            MARKET_OPEN_HOUR = 9
            MARKET_OPEN_MINUTE = 15
            MARKET_CLOSE_HOUR = 15
            MARKET_CLOSE_MINUTE = 30
            MARKET_MINUTES_PER_DAY = 375  # 9:15 to 15:30 = 375 minutes

            ranges = []
            minutes_collected = 0
            current_end = end_datetime

            while minutes_collected < total_minutes_needed:
                # Market hours for current day
                market_open = current_end.replace(
                    hour=MARKET_OPEN_HOUR, minute=MARKET_OPEN_MINUTE, second=0, microsecond=0
                )
                market_close = current_end.replace(
                    hour=MARKET_CLOSE_HOUR, minute=MARKET_CLOSE_MINUTE, second=0, microsecond=0
                )

                # Determine the end time for this range
                range_end = min(current_end, market_close)

                # Calculate how many minutes we can get from this day
                if range_end <= market_open:
                    # Current end is before market open, go to previous trading day
                    current_end = (current_end - timedelta(days=1)).replace(
                        hour=MARKET_CLOSE_HOUR, minute=MARKET_CLOSE_MINUTE
                    )
                    # Skip market holidays by checking for NIFTY 50 candles (handles weekends + holidays)
                    if short_code:
                        while not self._isTradingDay(current_end, short_code):
                            current_end = (current_end - timedelta(days=1)).replace(
                                hour=MARKET_CLOSE_HOUR, minute=MARKET_CLOSE_MINUTE
                            )
                    continue

                # Calculate available minutes from market_open to range_end
                available_minutes = int((range_end - market_open).total_seconds() / 60)

                # How many minutes to fetch from this day
                minutes_to_fetch = min(available_minutes, total_minutes_needed - minutes_collected)

                # Calculate start time for this range, snapped back to the nearest
                # candle boundary so the first fetched bar is always complete.
                range_start = range_end - timedelta(minutes=minutes_to_fetch)
                mins_from_open = int((range_start - market_open).total_seconds() / 60)
                snap = mins_from_open % candle_interval_minutes
                if snap != 0:
                    range_start -= timedelta(minutes=snap)
                range_start = max(range_start, market_open)  # Don't go before market open

                # Add this range
                ranges.insert(
                    0,
                    {  # Insert at beginning to maintain chronological order
                        "date": current_end.strftime("%Y-%m-%d"),
                        "from_time": range_start.strftime("%H:%M:%S"),
                        "to_time": range_end.strftime("%H:%M:%S"),
                    },
                )

                minutes_collected += minutes_to_fetch

                # Move to previous trading day's market close if we need more data
                if minutes_collected < total_minutes_needed:
                    current_end = (current_end - timedelta(days=1)).replace(
                        hour=MARKET_CLOSE_HOUR, minute=MARKET_CLOSE_MINUTE
                    )
                    # Skip market holidays by checking for NIFTY 50 candles (handles weekends + holidays)
                    if short_code:
                        while not self._isTradingDay(current_end, short_code):
                            current_end = (current_end - timedelta(days=1)).replace(
                                hour=MARKET_CLOSE_HOUR, minute=MARKET_CLOSE_MINUTE
                            )

            logging.debug(
                f"Fetch ranges for {num_candles} x {candle_interval_minutes}-min candles: {len(ranges)} range(s)"
            )
            for r in ranges:
                logging.debug(f"  {r['date']} from {r['from_time']} to {r['to_time']}")

            return ranges

        except Exception as e:
            logging.error(f"Error calculating fetch ranges: {str(e)}")
            import traceback

            traceback.print_exc()
            return []

    def _calcMA(self, candles, calculation_time, period, ma_type, candle_interval_minutes):
        """
        Pure MA calculation on a list of 1-min candle dicts.

        Args:
            candles: list of dicts with keys date, open, high, low, close
            calculation_time: datetime — bars containing or after this time are excluded
            period: number of bars for the MA
            ma_type: 'ema' or 'sma'
            candle_interval_minutes: size of each bar in minutes

        Returns:
            float or None
        """
        MARKET_OPEN_HOUR = 9
        MARKET_OPEN_MINUTE = 15

        grouped = {}
        for candle in candles:
            candle_time = candle["date"]
            if isinstance(candle_time, str):
                candle_time = datetime.strptime(candle_time, "%Y-%m-%d %H:%M:%S")
            market_open = candle_time.replace(
                hour=MARKET_OPEN_HOUR, minute=MARKET_OPEN_MINUTE, second=0, microsecond=0
            )
            mins = int((candle_time - market_open).total_seconds() / 60)
            bar_idx = mins // candle_interval_minutes
            day_key = (candle_time.date(), bar_idx)
            if day_key not in grouped:
                grouped[day_key] = []
            grouped[day_key].append(candle)

        calc_market_open = calculation_time.replace(
            hour=MARKET_OPEN_HOUR, minute=MARKET_OPEN_MINUTE, second=0, microsecond=0
        )
        current_bar_idx = (
            int((calculation_time - calc_market_open).total_seconds() / 60)
            // candle_interval_minutes
        )
        calc_date = calculation_time.date()

        bars = []
        for (d, idx) in sorted(grouped.keys()):
            if d > calc_date or (d == calc_date and idx >= current_bar_idx):
                break
            grp = grouped[(d, idx)]
            bars.append({
                "open": grp[0]["open"],
                "high": max(c["high"] for c in grp),
                "low": min(c["low"] for c in grp),
                "close": grp[-1]["close"],
            })

        if len(bars) < period:
            logging.debug(f"_calcMA: Not enough closed bars ({len(bars)} < {period})")
            return None

        closes = pd.Series([b["close"] for b in bars], dtype=float)

        if ma_type == "ema":
            series = ta.ema(closes, length=period)
        elif ma_type == "sma":
            series = ta.sma(closes, length=period)
        else:
            raise ValueError(f"Invalid ma_type: {ma_type}. Must be 'ema' or 'sma'")

        if series is None or series.empty:
            logging.error(f"_calcMA: pandas-ta {ma_type}() returned empty")
            return None

        value = series.iloc[-1]
        if pd.isna(value):
            return None

        return float(value)

    def _calcATR(self, candles, num_candles, candle_interval_minutes):
        """
        Pure ATR calculation on a list of 1-min candle dicts.

        Args:
            candles: list of dicts with keys date, open, high, low, close
            num_candles: ATR period
            candle_interval_minutes: size of each bar in minutes

        Returns:
            float or None
        """
        MARKET_OPEN_HOUR = 9
        MARKET_OPEN_MINUTE = 15

        grouped_candles = []
        current_group = []

        for candle in candles:
            candle_time = candle["date"]
            if isinstance(candle_time, str):
                candle_time = datetime.strptime(candle_time, "%Y-%m-%d %H:%M:%S")
            market_open = candle_time.replace(
                hour=MARKET_OPEN_HOUR, minute=MARKET_OPEN_MINUTE, second=0, microsecond=0
            )
            minutes_since_open = int((candle_time - market_open).total_seconds() / 60)
            interval_index = minutes_since_open // candle_interval_minutes

            if current_group:
                prev_candle_time = current_group[0]["date"]
                if isinstance(prev_candle_time, str):
                    prev_candle_time = datetime.strptime(prev_candle_time, "%Y-%m-%d %H:%M:%S")
                prev_market_open = prev_candle_time.replace(
                    hour=MARKET_OPEN_HOUR, minute=MARKET_OPEN_MINUTE, second=0, microsecond=0
                )
                prev_minutes = int((prev_candle_time - prev_market_open).total_seconds() / 60)
                prev_interval = prev_minutes // candle_interval_minutes

                if interval_index != prev_interval:
                    grouped_candles.append({
                        "open": current_group[0]["open"],
                        "high": max(c["high"] for c in current_group),
                        "low": min(c["low"] for c in current_group),
                        "close": current_group[-1]["close"],
                        "date": current_group[0]["date"],
                    })
                    current_group = []

            current_group.append(candle)

        if len(current_group) > 0:
            if len(current_group) < candle_interval_minutes:
                logging.debug(
                    f"_calcATR: Dropped partial last group: {len(current_group)}/{candle_interval_minutes} min"
                )
            else:
                grouped_candles.append({
                    "open": current_group[0]["open"],
                    "high": max(c["high"] for c in current_group),
                    "low": min(c["low"] for c in current_group),
                    "close": current_group[-1]["close"],
                    "date": current_group[0]["date"],
                })

        if len(grouped_candles) < num_candles + 1:
            logging.error(
                f"_calcATR: Need {num_candles + 1} grouped candles, got {len(grouped_candles)}"
            )
            return None

        df = pd.DataFrame(grouped_candles)
        df["open"] = df["open"].astype(float)
        df["high"] = df["high"].astype(float)
        df["low"] = df["low"].astype(float)
        df["close"] = df["close"].astype(float)

        atr_series = ta.atr(high=df["high"], low=df["low"], close=df["close"], length=num_candles)

        if atr_series is None or atr_series.empty:
            logging.error("_calcATR: pandas-ta atr() returned empty")
            return None

        atr = atr_series.iloc[-1]
        if pd.isna(atr):
            logging.error("_calcATR: ATR value is NaN")
            return None

        return float(atr)

    def _calcCMF(self, candles, calculation_time, period, candle_interval_minutes):
        """
        Pure CMF calculation on a list of 1-min candle dicts.

        Args:
            candles: list of dicts with keys date, open, high, low, close, volume
            calculation_time: datetime — bar containing this time is excluded
            period: CMF period
            candle_interval_minutes: size of each bar in minutes

        Returns:
            dict {'cmf': float, 'prev_cmf': float} or None
        """
        MARKET_OPEN = 9 * 60 + 15

        bars = {}
        for c in candles:
            ct = c["date"]
            if isinstance(ct, str):
                ct = datetime.strptime(ct, "%Y-%m-%d %H:%M:%S")
            mins = ct.hour * 60 + ct.minute - MARKET_OPEN
            bar_idx = mins // candle_interval_minutes
            day_key = (ct.date(), bar_idx)
            if day_key not in bars:
                bars[day_key] = []
            bars[day_key].append(c)

        now_mins = calculation_time.hour * 60 + calculation_time.minute - MARKET_OPEN
        current_bar_idx = now_mins // candle_interval_minutes
        calc_date = calculation_time.date()
        closed_bars = sorted(
            [
                (idx, cs) for (d, idx), cs in bars.items()
                if d < calc_date or (d == calc_date and idx < current_bar_idx)
            ],
            key=lambda x: (
                next(
                    (c["date"].date() if not isinstance(c["date"], str)
                     else datetime.strptime(c["date"], "%Y-%m-%d %H:%M:%S").date()
                     for c in x[1]),
                    calc_date
                ),
                x[0]
            ),
        )

        if len(closed_bars) < period + 1:
            logging.debug(
                f"_calcCMF: Insufficient closed bars: need {period + 1}, got {len(closed_bars)}"
            )
            return None

        def _bar_ohlcv(bar_candles):
            h = max(c["high"] for c in bar_candles)
            l = min(c["low"] for c in bar_candles)
            cl = bar_candles[-1]["close"]
            v = sum(c["volume"] for c in bar_candles)
            return h, l, cl, v

        def _cmf_for_window(window):
            mfv_sum = vol_sum = 0.0
            for _, cs in window:
                h, l, cl, v = _bar_ohlcv(cs)
                if h == l or v == 0:
                    continue
                mfm = ((cl - l) - (h - cl)) / (h - l)
                mfv_sum += mfm * v
                vol_sum += v
            return mfv_sum / vol_sum if vol_sum > 0 else 0.0

        current_cmf = _cmf_for_window(closed_bars[-period:])
        prev_cmf = _cmf_for_window(closed_bars[-period - 1:-1])

        return {"cmf": current_cmf, "prev_cmf": prev_cmf}

    def _calcVWAP(self, candles, candle_interval_minutes, anchor_time=None):
        """
        Pure VWAP + SD calculation on a list of 1-min candle dicts.

        Args:
            candles: list of dicts with keys date, open, high, low, close, volume
            candle_interval_minutes: size of each bar in minutes (1 = no grouping)
            anchor_time: optional datetime — VWAP resets from this time (default: market open)

        Returns:
            dict {'vwap': float, 'sd': float} or None
        """
        MARKET_OPEN_HOUR = 9
        MARKET_OPEN_MINUTE = 15

        if candle_interval_minutes > 1:
            # Derive market_open from the first candle's date
            first_time = candles[0]["date"]
            if isinstance(first_time, str):
                first_time = datetime.strptime(first_time, "%Y-%m-%d %H:%M:%S")
            market_open = first_time.replace(
                hour=MARKET_OPEN_HOUR, minute=MARKET_OPEN_MINUTE, second=0, microsecond=0
            )

            grouped_candles = []
            current_group = []

            for candle in candles:
                candle_time = candle["date"]
                if isinstance(candle_time, str):
                    candle_time = datetime.strptime(candle_time, "%Y-%m-%d %H:%M:%S")
                minutes_since_open = int((candle_time - market_open).total_seconds() / 60)
                interval_index = minutes_since_open // candle_interval_minutes

                if current_group:
                    prev_time = current_group[0]["date"]
                    if isinstance(prev_time, str):
                        prev_time = datetime.strptime(prev_time, "%Y-%m-%d %H:%M:%S")
                    prev_minutes = int((prev_time - market_open).total_seconds() / 60)
                    prev_interval = prev_minutes // candle_interval_minutes

                    if interval_index != prev_interval:
                        grouped_candles.append({
                            "open": current_group[0]["open"],
                            "high": max(c["high"] for c in current_group),
                            "low": min(c["low"] for c in current_group),
                            "close": current_group[-1]["close"],
                            "volume": sum(c["volume"] for c in current_group),
                            "date": current_group[0]["date"],
                        })
                        current_group = []

                current_group.append(candle)

            if len(current_group) == candle_interval_minutes:
                grouped_candles.append({
                    "open": current_group[0]["open"],
                    "high": max(c["high"] for c in current_group),
                    "low": min(c["low"] for c in current_group),
                    "close": current_group[-1]["close"],
                    "volume": sum(c["volume"] for c in current_group),
                    "date": current_group[0]["date"],
                })

            candles = grouped_candles

        df = pd.DataFrame(candles)
        df = df[df["volume"] > 0].copy()

        if df.empty:
            logging.error("_calcVWAP: No candles with volume > 0")
            return None

        df["open"] = df["open"].astype(float)
        df["high"] = df["high"].astype(float)
        df["low"] = df["low"].astype(float)
        df["close"] = df["close"].astype(float)
        df["volume"] = df["volume"].astype(float)

        df["typical_price"] = (df["high"] + df["low"] + df["close"]) / 3
        df["tp_volume"] = df["typical_price"] * df["volume"]
        df["cumulative_tp_volume"] = df["tp_volume"].cumsum()
        df["cumulative_volume"] = df["volume"].cumsum()
        df["vwap"] = df["cumulative_tp_volume"] / df["cumulative_volume"]
        df["squared_deviation"] = (df["typical_price"] - df["vwap"]) ** 2
        df["weighted_sq_dev"] = df["squared_deviation"] * df["volume"]
        df["cumulative_weighted_sq_dev"] = df["weighted_sq_dev"].cumsum()
        df["vwap_std"] = (df["cumulative_weighted_sq_dev"] / df["cumulative_volume"]).apply(
            lambda x: x**0.5
        )

        vwap = df["vwap"].iloc[-1]
        sd = df["vwap_std"].iloc[-1]

        if pd.isna(vwap):
            logging.error("_calcVWAP: VWAP is NaN")
            return None

        if pd.isna(sd):
            sd = 0.0

        return {"vwap": float(vwap), "sd": float(sd)}

    def getMA(
        self,
        tradingSymbol,
        short_code,
        calculation_time,
        period,
        ma_type="ema",
        units_per_candle=1,
        unit_type="minutes",
        isFnO=False,
        exchange="NFO",
        underlying=None,
        expiry_date=None,
    ):
        """
        Calculate a moving average (EMA or SMA) on the close of the last closed candle
        before calculation_time.

        Args:
            tradingSymbol: Trading symbol to calculate MA for
            short_code: Broker short code
            calculation_time: Reference datetime — MA is calculated using closed candles
                              strictly before this time. Can be datetime or 'YYYY-MM-DD HH:MM:SS'.
            period: Number of candles for the MA (e.g. 5 for EMA(5), 25 for SMA(25))
            ma_type: 'ema' (default) or 'sma'
            units_per_candle: Minutes per candle (e.g. 3 for 3-min candles)
            unit_type: 'minutes' or 'hours'
            isFnO: Whether it's an F&O instrument
            exchange: Exchange name (NFO, BFO, NSE, etc.)
            underlying: Optional underlying symbol for ICICI
            expiry_date: Optional expiry date in YYYY-MM-DD for ICICI

        Returns:
            float: MA value of the last closed candle, or None on error

        Examples:
            # 5 EMA on 3-min candles of an option, as of now
            ema = quotes.getMA('NIFTY24MAR24500CE', 'jitesing', datetime.now(),
                               period=5, ma_type='ema', units_per_candle=3,
                               isFnO=True, exchange='NFO')

            # 25 SMA on 3-min candles
            sma = quotes.getMA('NIFTY24MAR24500CE', 'jitesing', datetime.now(),
                               period=25, ma_type='sma', units_per_candle=3,
                               isFnO=True, exchange='NFO')
        """
        try:
            if isinstance(calculation_time, str):
                calculation_time = datetime.strptime(calculation_time, "%Y-%m-%d %H:%M:%S")

            if unit_type == "hours":
                candle_interval_minutes = units_per_candle * 60
            elif unit_type == "minutes":
                candle_interval_minutes = units_per_candle
            else:
                raise ValueError(f"Invalid unit_type: {unit_type}. Must be 'minutes' or 'hours'")

            # Fetch enough 1-min candles: period * 2 for EMA warmup, +1 for reference
            fetch_ranges = self.calculateFetchRanges(
                calculation_time=calculation_time,
                num_candles=period * 2 + 1,
                units_per_candle=units_per_candle,
                unit_type=unit_type,
                short_code=short_code,
            )

            if not fetch_ranges:
                logging.error(f"getMA: Could not calculate fetch ranges for {tradingSymbol}")
                return None

            all_candles = []
            for fetch_range in fetch_ranges:
                candles = self.getHistoricalData(
                    tradingSymbol=tradingSymbol,
                    short_code=short_code,
                    date_str=fetch_range["date"],
                    isFnO=isFnO,
                    exchange=exchange,
                    from_time=fetch_range["from_time"],
                    to_time=fetch_range["to_time"],
                    underlying=underlying,
                    expiry_date=expiry_date,
                )
                if candles:
                    all_candles.extend(candles)

            if not all_candles:
                logging.error(f"getMA: No candle data for {tradingSymbol} at {calculation_time}")
                return None

            value = self._calcMA(all_candles, calculation_time, period, ma_type, candle_interval_minutes)
            if value is not None:
                logging.debug(
                    f"getMA: {ma_type.upper()}({period}, {units_per_candle}min) for {tradingSymbol} at {calculation_time}: {value:.2f}"
                )
            return value

        except Exception as e:
            logging.error(f"getMA: Error for {tradingSymbol}: {e}")
            import traceback

            traceback.print_exc()
            return None

    def getATR(
        self,
        tradingSymbol,
        short_code,
        calculation_time,
        num_candles,
        units_per_candle=1,
        unit_type="minutes",
        isFnO=False,
        exchange="NFO",
        underlying=None,
        expiry_date=None,
    ):
        """
        Calculate ATR (Average True Range) by looking back X candles from a specific datetime.
        Fetches historical data and caches in QuestDB and in-memory.

        ATR is calculated using the previous num_candles before calculation_time:
        1. Use calculateFetchRanges to determine data ranges to fetch (respecting market hours)
        2. Fetch historical 1-minute candles for those ranges
        3. Group into larger candles based on units_per_candle
        4. Calculate True Range: TR = max(high-low, |high-prev_close|, |low-prev_close|)
        5. ATR = average of all TRs using pandas-ta

        Args:
            tradingSymbol: Trading symbol to calculate ATR for
            short_code: Broker short code
            calculation_time: The reference datetime - ATR will be calculated using candles BEFORE this time
                              Can be datetime object or string 'YYYY-MM-DD HH:MM:SS'
            num_candles: Number of candles to use for ATR calculation (e.g., 3 for ATR(3))
            units_per_candle: How many time units form a single candle (default: 1)
                              Use 3 for 3-minute candles, 5 for 5-minute, 60 for 1-hour, etc.
            unit_type: Type of time unit - 'minutes' or 'hours' (default: 'minutes')
            isFnO: Whether it's F&O instrument
            exchange: Exchange name (NFO for F&O, NSE for equity)
            underlying: Optional underlying symbol name (e.g., 'NIFTY', 'BANKNIFTY') - for ICICI
            expiry_date: Optional expiry date in YYYY-MM-DD format - for ICICI

        Returns:
            float: ATR value, or None on error

        Examples:
            # ATR using 3 three-minute candles ending at 9:24
            at_dt = datetime(2024, 3, 23, 9, 24)
            atr = Quotes.getATR('NIFTY 50', 'jitesing', at_dt, num_candles=3, units_per_candle=3, unit_type='minutes')

            # ATR using 14 one-minute candles ending at 10:00
            at_dt = '2024-03-23 10:00:00'
            atr = Quotes.getATR('NIFTY 50', 'jitesing', at_dt, num_candles=14, units_per_candle=1)

            # ATR using 5 hourly candles ending at 14:00
            at_dt = datetime(2024, 3, 23, 14, 0)
            atr = Quotes.getATR('BANKNIFTY', 'jitesing', at_dt, num_candles=5, units_per_candle=1, unit_type='hours')
        """
        try:
            # Convert string datetime to datetime object if needed
            if isinstance(calculation_time, str):
                calculation_time = datetime.strptime(calculation_time, "%Y-%m-%d %H:%M:%S")

            # Convert unit_type to minutes for internal calculation
            if unit_type == "hours":
                candle_interval_minutes = units_per_candle * 60
            elif unit_type == "minutes":
                candle_interval_minutes = units_per_candle
            else:
                raise ValueError(f"Invalid unit_type: {unit_type}. Must be 'minutes' or 'hours'")

            logging.debug(
                f"ATR calculation: {num_candles} x {units_per_candle}-{unit_type} candles ending at {calculation_time}"
            )

            # We need num_candles * 5 + 1 candles: warmup (num_candles*4) + target (num_candles) + 1 reference
            # Extra warmup candles allow Wilder's RMA to converge before the target window
            # At 4x warmup, seed weight is (13/14)^(4*14) < 1% — negligible for SL placement
            fetch_ranges = self.calculateFetchRanges(
                calculation_time=calculation_time,
                num_candles=num_candles * 5 + 1,  # 5x for warmup + reference
                units_per_candle=units_per_candle,
                unit_type=unit_type,
                short_code=short_code,
            )

            if not fetch_ranges:
                logging.error(f"Could not calculate fetch ranges for {tradingSymbol}")
                return None

            # Fetch 1-minute candles for all ranges
            all_candles = []
            for fetch_range in fetch_ranges:
                candles = self.getHistoricalData(
                    tradingSymbol=tradingSymbol,
                    short_code=short_code,
                    date_str=fetch_range["date"],
                    isFnO=isFnO,
                    exchange=exchange,
                    from_time=fetch_range["from_time"],
                    to_time=fetch_range["to_time"],
                    underlying=underlying,
                    expiry_date=expiry_date,
                )

                if candles:
                    all_candles.extend(candles)

            if not all_candles:
                logging.error(
                    f"No data available for ATR calculation for {tradingSymbol} ending at {calculation_time}"
                )
                return None

            logging.debug(f"Fetched {len(all_candles)} 1-minute candles for ATR calculation")

            atr = self._calcATR(all_candles, num_candles, candle_interval_minutes)
            if atr is not None:
                logging.info(f"ATR({num_candles}) for {tradingSymbol} at {calculation_time}: {atr:.2f}")
            return atr

        except Exception as e:
            logging.error(f"Error calculating ATR for {tradingSymbol}: {str(e)}")
            import traceback

            traceback.print_exc()
            return None

    def getCMF(
        self,
        tradingSymbol,
        short_code,
        calculation_time,
        period=20,
        units_per_candle=3,
        unit_type="minutes",
        isFnO=False,
        exchange="NFO",
        underlying=None,
        expiry_date=None,
    ):
        """
        Calculate Chaikin Money Flow (CMF) for the last closed candle before calculation_time.

        CMF(n) = sum(MFV, n) / sum(volume, n)
        where MFV = ((close - low) - (high - close)) / (high - low) * volume

        Returns a dict {cmf: float, prev_cmf: float} so the caller can detect crossovers,
        or None if there is insufficient data.

        Args:
            tradingSymbol:    Trading symbol
            short_code:       Broker short code
            calculation_time: Reference datetime — only closed candles before this time are used
            period:           Number of candles for CMF (default 20)
            units_per_candle: Minutes per candle (default 3)
            unit_type:        'minutes' or 'hours'
            isFnO:            F&O instrument flag
            exchange:         Exchange (NFO, BFO, NSE, …)
            underlying:       Optional underlying symbol for ICICI fallback
            expiry_date:      Optional expiry date (YYYY-MM-DD) for ICICI fallback

        Returns:
            dict with keys 'cmf' (current) and 'prev_cmf' (one candle earlier), or None on error
        """
        try:
            if isinstance(calculation_time, str):
                calculation_time = datetime.strptime(calculation_time, "%Y-%m-%d %H:%M:%S")

            if unit_type == "hours":
                candle_interval_minutes = units_per_candle * 60
            elif unit_type == "minutes":
                candle_interval_minutes = units_per_candle
            else:
                raise ValueError(f"Invalid unit_type: {unit_type}")

            # Need period + 1 closed candles (current + previous for crossover detection)
            fetch_ranges = self.calculateFetchRanges(
                calculation_time=calculation_time,
                num_candles=period + 1,
                units_per_candle=units_per_candle,
                unit_type=unit_type,
                short_code=short_code,
            )
            if not fetch_ranges:
                logging.error(f"getCMF: Could not calculate fetch ranges for {tradingSymbol}")
                return None

            all_candles = []
            for fr in fetch_ranges:
                candles = self.getHistoricalData(
                    tradingSymbol=tradingSymbol,
                    short_code=short_code,
                    date_str=fr["date"],
                    isFnO=isFnO,
                    exchange=exchange,
                    from_time=fr["from_time"],
                    to_time=fr["to_time"],
                    underlying=underlying,
                    expiry_date=expiry_date,
                )
                if candles:
                    all_candles.extend(candles)

            if not all_candles:
                logging.error(f"getCMF: No candle data for {tradingSymbol} at {calculation_time}")
                return None

            result = self._calcCMF(all_candles, calculation_time, period, candle_interval_minutes)
            if result is not None:
                logging.debug(
                    f"getCMF({period}) for {tradingSymbol} at {calculation_time}: "
                    f"cmf={result['cmf']:.4f} prev={result['prev_cmf']:.4f}"
                )
            return result

        except Exception as e:
            logging.error(f"getCMF: Error for {tradingSymbol}: {e}")
            import traceback

            traceback.print_exc()
            return None

    def getVWAP(
        self,
        tradingSymbol,
        short_code,
        calculation_time,
        units_per_candle=1,
        unit_type="minutes",
        isFnO=False,
        exchange="NFO",
        underlying=None,
        expiry_date=None,
        anchor_time=None,
    ):
        """
        Calculate VWAP (Volume Weighted Average Price) and Standard Deviation for intraday data.
        VWAP is calculated from market open (9:15 AM) to calculation_time on the same day.
        When anchor_time is provided, VWAP is calculated from anchor_time instead of market open.

        VWAP = Cumulative(typical_price × volume) / Cumulative(volume)
        where typical_price = (high + low + close) / 3

        SD = Expanding standard deviation of VWAP values from market open

        Args:
            tradingSymbol: Trading symbol to calculate VWAP for
            short_code: Broker short code
            calculation_time: The reference datetime - VWAP calculated from 9:15 AM to this time
                              Can be datetime object or string 'YYYY-MM-DD HH:MM:SS'
                              Must be between 9:15 AM and 3:30 PM on a weekday
            units_per_candle: How many time units form a single candle (default: 1)
                              Use 1 for 1-minute candles (recommended for VWAP accuracy)
                              Use 3 for 3-minute, 5 for 5-minute, 60 for 1-hour, etc.
            unit_type: Type of time unit - 'minutes' or 'hours' (default: 'minutes')
            isFnO: Whether it's F&O instrument
            exchange: Exchange name (NFO for F&O, NSE for equity)
            underlying: Optional underlying symbol name (e.g., 'NIFTY', 'BANKNIFTY') - for ICICI
            expiry_date: Optional expiry date in YYYY-MM-DD format - for ICICI

        Returns:
            dict: {'vwap': float, 'sd': float} or None on error
                  - vwap: Volume Weighted Average Price from 9:15 AM to calculation_time
                  - sd: Standard deviation of VWAP values (expanding window from 9:15 AM)

        Examples:
            # VWAP from market open (9:15 AM) to 10:00 AM using 1-minute candles
            result = Quotes.getVWAP('NIFTY 50', 'jitesing', '2024-03-23 10:00:00')
            # result = {'vwap': 22456.78, 'sd': 23.45}

            # With 3-minute candles
            result = Quotes.getVWAP('NIFTY 50', 'jitesing', '2024-03-23 14:00:00',
                                    units_per_candle=3)

            # F&O instrument
            result = Quotes.getVWAP('NIFTY24FEB24500CE', 'jitesing', datetime.now(),
                                    isFnO=True, exchange='NFO',
                                    underlying='NIFTY', expiry_date='2024-02-24')

            # Usage in strategy
            if result:
                vwap = result['vwap']
                sd = result['sd']
                current_price = Quotes.getCMP('NIFTY 50', 'jitesing')
                if current_price > vwap + (1.5 * sd):
                    # Price above VWAP + 1.5 SD - potential bearish signal
                    pass

        Note:
            - VWAP is most accurate with 1-minute candles (default)
            - SD requires at least 2 data points (will be 0 for single candle)
            - VWAP always resets at market open (9:15 AM) - intraday only
            - Candles with zero volume are filtered out before calculation
            - calculation_time must be on a weekday between 9:15 AM and 3:30 PM
        """
        try:
            # Step 1: Input Validation and Conversion
            if isinstance(calculation_time, str):
                calculation_time = datetime.strptime(calculation_time, "%Y-%m-%d %H:%M:%S")

            if calculation_time.second > 1:
                calculation_time = calculation_time.replace(second=1, microsecond=0)

            # Validate weekday
            if calculation_time.weekday() >= 5:
                logging.error(f"calculation_time {calculation_time} is on a weekend")
                return None

            # Convert unit_type to minutes
            if unit_type == "hours":
                candle_interval_minutes = units_per_candle * 60
            elif unit_type == "minutes":
                candle_interval_minutes = units_per_candle
            else:
                raise ValueError(f"Invalid unit_type: {unit_type}. Must be 'minutes' or 'hours'")

            # Market hours constants
            MARKET_OPEN_HOUR = 9
            MARKET_OPEN_MINUTE = 15
            MARKET_CLOSE_HOUR = 15
            MARKET_CLOSE_MINUTE = 30

            # Create market open and close times for calculation_time's date
            # Preserve timezone info from calculation_time (if present)
            market_open = calculation_time.replace(
                hour=MARKET_OPEN_HOUR, minute=MARKET_OPEN_MINUTE, second=0, microsecond=0
            )
            market_close = calculation_time.replace(
                hour=MARKET_CLOSE_HOUR, minute=MARKET_CLOSE_MINUTE, second=0, microsecond=0
            )

            # Validate calculation_time is within market hours
            if calculation_time < market_open:
                logging.error(
                    f"calculation_time {calculation_time} is before market open at {market_open}"
                )
                return None

            if calculation_time > market_close:
                logging.error(
                    f"calculation_time {calculation_time} is after market close at {market_close}"
                )
                return None

            # Determine fetch start: anchor_time if provided, else market open
            if anchor_time is not None:
                fetch_start = calculation_time.replace(
                    hour=anchor_time.hour, minute=anchor_time.minute, second=0, microsecond=0
                )
                if fetch_start < market_open:
                    fetch_start = market_open
            else:
                fetch_start = market_open

            logging.debug(
                f"VWAP calculation for {tradingSymbol}: from {fetch_start} to {calculation_time}"
            )

            # Step 2: Fetch Historical Data
            date_str = calculation_time.strftime("%Y-%m-%d")
            from_time = fetch_start.strftime("%H:%M:%S")
            to_time = calculation_time.strftime("%H:%M:%S")

            logging.debug(f"Fetching data for {date_str} from {from_time} to {to_time}")

            candles = self.getHistoricalData(
                tradingSymbol=tradingSymbol,
                short_code=short_code,
                date_str=date_str,
                isFnO=isFnO,
                exchange=exchange,
                from_time=from_time,
                to_time=to_time,
                underlying=underlying,
                expiry_date=expiry_date,
            )

            if not candles or len(candles) == 0:
                logging.error(
                    f"No data available for VWAP calculation for {tradingSymbol} on {date_str}"
                )
                return None

            logging.debug(f"Fetched {len(candles)} 1-minute candles for VWAP calculation")

            result = self._calcVWAP(candles, candle_interval_minutes, anchor_time)
            if result is not None:
                logging.info(
                    f"VWAP for {tradingSymbol} from {market_open} to {calculation_time}: VWAP={result['vwap']:.2f}, SD={result['sd']:.2f}"
                )
            return result

        except Exception as e:
            logging.error(f"Error calculating VWAP for {tradingSymbol}: {str(e)}")
            import traceback

            traceback.print_exc()
            return None
