import json
import logging
import os
import time
from datetime import datetime
import traceback
from trademgmt.TradeManager import TradeManager
from trademgmt.BacktestOrderManager import BacktestOrderManager
from config.Config import getBrokerAppConfig, getServerConfig
from trademgmt.TradeExitReason import TradeExitReason
from utils.Utils import Utils


class BacktestManager(TradeManager):
    """
    BacktestManager extends TradeManager to support backtesting strategies
    on historical data instead of live trading.
    """

    def __init__(self, group=None, target=None, name=None, args=(), kwargs=None):
        """
        Initialize BacktestManager with additional parameters for backtesting
        args should contain: (accessToken, algoConfig, test_date)
        """
        # Extract backtest-specific parameters
        if len(args) >= 5:
            self._accessToken, self.algoConfig, self.test_date, self.run_timestamp, self.short_code = args[:5]
        elif len(args) >= 4:
            self._accessToken, self.algoConfig, self.test_date, self.run_timestamp = args[:4]
            self.short_code = name.split("_")[0] if name else None
        elif len(args) == 3:
            self._accessToken, self.algoConfig, self.test_date = args[:3]
            self.run_timestamp = datetime.now().strftime('%Y%m%dT%H%M%S')
        else:
            raise ValueError(
                "BacktestManager requires: accessToken, algoConfig, test_date")

        # Initialize parent with modified args (only accessToken and algoConfig)
        super().__init__(group=group, target=target, name=name,
                         args=(self._accessToken, self.algoConfig), kwargs=kwargs)

        # Backtest-specific attributes
        self.current_date = self.test_date
        self.timestamp_maps = {}
        self.backtest_results = {
            'trades': [],
            'total_pnl': 0,
            'total_trades': 0
        }
        self.is_backtest_mode = True
        self.quotes.candles_table = "nobroker_candles"
        self.aggressive_mode = False

        logging.info(f'BacktestManager initialized for date: {self.test_date}')

    def run(self):
        """Thread entry point. Runs the candle loop and generates the report."""
        logging.info(f'{self.__class__.__name__}: Processing backtest for {self.test_date}')

        nifty_timestamp_map = self.timestamp_maps.get("NIFTY 50", {})

        if not nifty_timestamp_map:
            logging.warning('%s: No NIFTY 50 data available, skipping day', self.__class__.__name__)
            return

        nifty_candles = [nifty_timestamp_map[ts] for ts in sorted(nifty_timestamp_map.keys())]

        logging.info(f'{self.__class__.__name__}: Processing {len(nifty_candles)} candles based on NIFTY 50')

        for candle_index, nifty_candle in enumerate(nifty_candles):
            current_timestamp = nifty_candle['date']

            self.symbolToCMPMap["NIFTY 50"] = nifty_candle['close']
            self.symbolToCMPMap['exchange_timestamp'] = current_timestamp

            for symbol, timestamp_map in self.timestamp_maps.items():
                if current_timestamp in timestamp_map:
                    self.symbolToCMPMap[symbol] = timestamp_map[current_timestamp]['close']

            if candle_index == 0:
                self._processStrategies(current_timestamp)
                self.simulateTickerUpdates(nifty_candle, current_timestamp)
                continue

            if candle_index % 60 == 0:
                logging.info(
                    f'{self.__class__.__name__}: Processed candle {candle_index}/{len(nifty_candles)} - Time: {current_timestamp}')

                if all(not s.isEnabled() for s in self.strategyToInstanceMap.values()) and len(self.strategyToInstanceMap) > 0:
                    logging.info(
                        f'{self.__class__.__name__}: All strategies are disabled at {current_timestamp}. Ending backtest early.')
                    break

            self._refreshActiveTradePnl()
            self._processStrategies(current_timestamp)

            try:
                self.simulateTickerUpdates(nifty_candle, current_timestamp)
                self.checkStrategyHealth()
                self.fetchAndUpdateAllTradeOrders()
                self.trackAndUpdateAllTrades()
            except RuntimeError:
                raise
            except Exception as e:
                traceback.print_exc()
                logging.exception("Exception in BacktestManager Main thread")

        logging.info(f'{self.__class__.__name__}: Finished processing all {len(nifty_candles)} candles for {self.test_date}')

        self.generateBacktestReport()

        if self.backtest_results['total_trades'] == 0:
            logging.info(f'Backtest completed for {self.test_date} | No strategies ran')
        else:
            logging.info(f'Backtest completed for {self.test_date} | PnL: {self.backtest_results["total_pnl"]:>+15,.2f}')

    def setupBacktestEnvironment(self):
        """
        Setup environment for backtesting (directories, data loading, etc.)
        """
        serverConfig = getServerConfig()
        tradesDir = os.path.join(serverConfig['deployDir'], 'backtest_results')

        os.makedirs(tradesDir, exist_ok=True)

        self.backtest_results_dir = tradesDir

    def loadIndexHistoricalData(self):
        """
        Load historical data for major indices for the test date
        """
        # Define indices with their exchange info
        indices = [
            ("NIFTY 50", False, "NSE"),
            ("NIFTY BANK", False, "NSE"),
            ("INDIA VIX", False, "NSE"),
            ("SENSEX", False, "BSE")
        ]

        for symbol, isFnO, exchange in indices:
            if not self._loadHistoricalData(
                    symbol, isFnO=isFnO, exchange=exchange):
                return


        logging.info(
            f'{self.__class__.__name__}: Completed loading historical data for {self.test_date}')

    def _loadHistoricalData(self, tradingSymbol, isFnO=None, exchange=None, from_time=None, to_time=None, underlying=None, expiry_date=None):
        """
        Unified method to load historical data for any symbol.
        All fetched candles are cached in timestamp_maps for O(1) lookup.
        QuestDB is the persistent cache layer (handled by Quotes.getHistoricalData).

        Args:
            tradingSymbol: Symbol to load data for
            isFnO: Whether it's F&O instrument (if None, will try to detect)
            exchange: Exchange name ('NSE' or 'NFO') (if None, will try to detect)
            from_time: Optional start time (HH:MM:SS) for single candle fetch
            to_time: Optional end time (HH:MM:SS) for single candle fetch
            underlying: Optional underlying symbol for options (e.g., 'NIFTY', 'BANKNIFTY')
            expiry_date: Optional expiry date in YYYY-MM-DD format

        Returns:
            list of candles or None if failed
        """
        try:
            # Fetch from QuestDB or API (QuestDB cache is handled by Quotes.getHistoricalData)
            candles = self.quotes.getHistoricalData(
                tradingSymbol=tradingSymbol,
                short_code=self.short_code,
                date_str=self.test_date,
                isFnO=isFnO,
                exchange=exchange,
                from_time=from_time,
                to_time=to_time,
                underlying=underlying,
                expiry_date=expiry_date
            )

            time.sleep(0.35)  # Rate limit: stay under Zerodha's 3 req/s during backtesting

            if candles and len(candles) > 0:
                # Append to timestamp_maps — merges with any existing candles for this symbol
                if tradingSymbol not in self.timestamp_maps:
                    self.timestamp_maps[tradingSymbol] = {}
                for candle in candles:
                    self.timestamp_maps[tradingSymbol][candle['date']] = candle

                logging.info(
                    f'{self.__class__.__name__}: Loaded {len(candles)} candles for {tradingSymbol}')
                return candles
            else:
                logging.warning(
                    f'{self.__class__.__name__}: No historical data available for {tradingSymbol} on {self.test_date}')

        except Exception as e:
            logging.error(
                f'{self.__class__.__name__}: Failed to load historical data for {tradingSymbol}: {str(e)}')

        return None

    def _loadSingleCandle(self, tradingSymbol, timestamp, isFnO=None, exchange=None, underlying=None, expiry_date=None):
        """
        Load a single candle for a specific symbol at a specific timestamp
        Used for lightweight quote checking before trade placement
        Returns the candle dict or None if not found

        Checks in-memory cache (timestamp_maps) first,
        then falls back to QuestDB/API.
        """
        from datetime import datetime

        # 1. Check timestamp_maps (O(1) lookup)
        if tradingSymbol in self.timestamp_maps:
            candle = self.timestamp_maps[tradingSymbol].get(timestamp)
            if candle:
                logging.debug(
                    f'{self.__class__.__name__}: Found single candle in timestamp_maps for {tradingSymbol} at {timestamp}')
                return candle

        # 2. Fall back to QuestDB / API
        # Extract time from timestamp
        if isinstance(timestamp, datetime):
            time_str = timestamp.strftime('%H:%M:%S')
        else:
            time_str = timestamp.strftime('%H:%M:%S') if hasattr(
                timestamp, 'strftime') else None

        if not time_str:
            logging.error(
                f'{self.__class__.__name__}: Invalid timestamp format for {tradingSymbol}')
            return None

        # Use unified method to fetch single candle (don't cache in maps)
        candles = self._loadHistoricalData(
            tradingSymbol,
            isFnO=isFnO,
            exchange=exchange,
            from_time=time_str,
            to_time=time_str,
            underlying=underlying,
            expiry_date=expiry_date
        )

        if candles and len(candles) > 0:
            logging.debug(
                f'{self.__class__.__name__}: Loaded single candle from DB/API for {tradingSymbol} at {timestamp}')
            return candles[0]

        return None

    def updateCandle(self, tick):
        """No-op in backtest mode. Prevents writing flat single-tick candles back to
        QuestDB, which would overwrite real OHLC data cached from the broker API."""
        pass

    def startStrategyExecution(self, strategyInstance, threadName):
        """Backtest mode: no threads. Strategy is already registered via its constructor.
        run() drives process() calls directly."""
        pass

    def _processStrategies(self, current_timestamp):
        """Call process() on each active strategy — single-threaded, deterministic."""
        for strategy in list(self.strategyToInstanceMap.values()):
            if not strategy.isEnabled() or not strategy.canTradeToday():
                continue
            if current_timestamp < strategy.startTimestamp:
                continue
            if current_timestamp > strategy.squareOffTimestamp:
                strategy.setDisabled()
                continue
            sl_or_target = strategy.isTargetORSLHit()
            if sl_or_target is not None:
                self.squareOffStrategy(strategy, sl_or_target)
                continue
            try:
                strategy.process()
            except RuntimeError as e:
                logging.error("Fatal error in %s.process(): %s", strategy.getName(), str(e), exc_info=True)
                self.squareOffStrategy(strategy, TradeExitReason.STRATEGY_ERROR)
                raise
            except Exception as e:
                logging.error("Exception in %s.process(): %s", strategy.getName(), str(e), exc_info=True)
                self.squareOffStrategy(strategy, TradeExitReason.STRATEGY_ERROR)

    def simulateTickerUpdates(self, nifty_candle, current_timestamp):
        """
        Simulate ticker listener calls for all symbols with historical data
        This allows strategies to react to price updates in backtesting

        Note: Only sends ticks for symbols that have actual data at this timestamp.
        If a symbol has no data (illiquid option), its price in symbolToCMPMap stays
        at the last known value, and no tick is sent. This mimics real market behavior
        where illiquid instruments don't tick every minute.
        """
        from models.TickData import TickData

        # First, check and execute SL orders for all active trades BEFORE triggering trade logic
        # Build candle_data_map from timestamp_maps for current timestamp
        candle_data_map = {}
        for symbol, timestamp_map in self.timestamp_maps.items():
            if current_timestamp in timestamp_map:
                candle_data_map[symbol] = timestamp_map[current_timestamp]

        # Call the order manager to check SL orders
        orderManager = self.getOrderManager(self.short_code)
        if hasattr(orderManager, 'checkAndExecuteSLOrders'):
            orderManager.checkAndExecuteSLOrders(self.trades, candle_data_map)

        # Create tick for NIFTY 50 (always present as reference)
        # Set exchange_timestamp here - it will be the same for all ticks in this iteration
        nifty_tick = TickData("NIFTY 50")
        nifty_tick.lastTradedPrice = nifty_candle['close']
        nifty_tick.open = nifty_candle['open']
        nifty_tick.high = nifty_candle['high']
        nifty_tick.low = nifty_candle['low']
        nifty_tick.close = nifty_candle['close']
        nifty_tick.volume = nifty_candle.get('volume', 0)
        nifty_tick.exchange_timestamp = current_timestamp
        self.tickerListener(nifty_tick)

        # Simulate ticks for all other symbols that have data at this timestamp
        # Create a snapshot of items to avoid "dictionary changed size during iteration" error
        timestamp_maps_snapshot = list(self.timestamp_maps.items())
        ticked_symbols = {"NIFTY 50"}  # Track symbols that already got ticks

        for symbol, timestamp_map in timestamp_maps_snapshot:
            ticked_symbols.add(symbol)
            if current_timestamp in timestamp_map:
                candle = timestamp_map[current_timestamp]

                tick = TickData(symbol)
                tick.lastTradedPrice = candle['close']
                tick.open = candle['open']
                tick.high = candle['high']
                tick.low = candle['low']
                tick.close = candle['close']
                tick.volume = candle.get('volume', 0)
                tick.exchange_timestamp = current_timestamp

                # Call ticker listener to trigger strategy logic
                self.tickerListener(tick)
            # If symbol has no data at this timestamp:
            # - symbolToCMPMap keeps last known price (forward-fill)
            # - No tick is sent (realistic for illiquid options)
            # - Strategies won't get triggered for this symbol

        # Load single candles for registered symbols not yet in timestamp_maps
        # These are symbols from trades placed during the backtest that don't have full-day data
        for symbol in list(self.registeredSymbols):
            if symbol in ticked_symbols:
                continue

            # Find the trade to get metadata (isFnO, exchange, underlying, expiryDay)
            trade = None
            for tr in self.trades:
                if tr.tradingSymbol == symbol:
                    trade = tr
                    break

            isFnO = (trade.isOptions or trade.isFutures) if trade else None
            exchange = trade.exchange if trade else None
            underlying = trade.underLying if trade else None
            expiry_date = None

            if underlying and isFnO:
                from datetime import datetime as dt
                from utils.Utils import Utils as UtilsLocal
                strategy_instance = self.strategyToInstanceMap.get(trade.strategy) if trade else None
                test_datetime = dt.strptime(self.test_date, '%Y-%m-%d')

                # Use strategy's own getExpiryDate if available (handles monthly/weekly)
                if strategy_instance and hasattr(strategy_instance, 'getExpiryDate'):
                    expiry_datetime = strategy_instance.getExpiryDate(datetimeObj=test_datetime)
                elif strategy_instance and hasattr(strategy_instance, 'expiryDay'):
                    # Fallback to weekly expiry using strategy's expiryDay
                    expiry_datetime = UtilsLocal.getWeeklyExpiryDayDate(
                        underlying, dateTimeObj=test_datetime, expiryDay=strategy_instance.expiryDay)
                else:
                    logging.error(
                        f'{self.__class__.__name__}: Strategy {trade.strategy if trade else "unknown"} has no getExpiryDate or expiryDay - cannot determine expiry')
                    continue

                expiry_date = expiry_datetime.strftime('%Y-%m-%d')

            candle = self._loadSingleCandle(
                symbol,
                current_timestamp,
                isFnO=isFnO,
                exchange=exchange,
                underlying=underlying,
                expiry_date=expiry_date
            )

            if candle:
                self.symbolToCMPMap[symbol] = candle['close']
                candle_data_map[symbol] = candle

                tick = TickData(symbol)
                tick.lastTradedPrice = candle['close']
                tick.open = candle['open']
                tick.high = candle['high']
                tick.low = candle['low']
                tick.close = candle['close']
                tick.volume = candle.get('volume', 0)
                tick.exchange_timestamp = current_timestamp
                self.tickerListener(tick)

    def generateBacktestReport(self):
        """
        Write per-day strategy metadata JSON and append trades to the run-level CSV.
        All calculations (PnL totals, win rates, Kelly, etc.) are done client-side in JS.
        """
        import csv

        report_file = os.path.join(
            self.backtest_results_dir,
            f'backtest_{self.test_date}_{self.run_timestamp}.json'
        )
        csv_file = os.path.join(
            self.backtest_results_dir,
            f'trades_{self.run_timestamp}.csv'
        )

        # Per-strategy metadata: days_to_expiry, highest_pnl, lowest_pnl
        strategy_meta = {}
        for strategy, instance in self.strategyToInstanceMap.items():
            strategy_meta[strategy] = {
                'days_to_expiry': getattr(instance, 'daysToExpiry', None),
                'highest_pnl': getattr(instance, 'highestPnl', 0),
                'lowest_pnl': getattr(instance, 'lowestPnl', 0),
            }

        report = {
            'date': self.test_date,
            'highest_pnl': self.dayHighestPnl,
            'lowest_pnl': self.dayLowestPnl,
            'strategies': strategy_meta,
        }

        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2, default=str)

        # Append trades to the run-level CSV (write header only on first write)
        write_header = not os.path.exists(csv_file)
        csv_columns = [
            'Index', 'Entry-Date', 'Entry-Weekday', 'Entry-Time', 'Entry-Price',
            'Quantity', 'Instrument-Kind', 'StrikePrice', 'Position',
            'ExitDate', 'Exit-Weekday', 'ExitTime', 'ExitPrice',
            'P/L', 'Brokerage', 'P/L-Percentage', 'ExpiryDate',
            'Highest MTM(Candle Close)', 'Lowest MTM(Candle Close)',
            'Remarks[exit reason]', 'Strategy', 'Symbol',
        ]

        # Determine starting index by counting existing data rows
        start_index = 1
        if not write_header:
            with open(csv_file, 'r') as f_count:
                start_index = sum(1 for _ in f_count)  # header + existing rows = next index

        with open(csv_file, 'a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=csv_columns)
            if write_header:
                writer.writeheader()
            for i, t in enumerate(self.backtest_results['trades'], start=start_index):
                writer.writerow({
                    'Index': i,
                    'Entry-Date': t['date'],
                    'Entry-Weekday': t['weekday'],
                    'Entry-Time': t['entry_time'],
                    'Entry-Price': t['entry'],
                    'Quantity': t['qty'],
                    'Instrument-Kind': t['instrument_kind'],
                    'StrikePrice': t['strike_price'],
                    'Position': t['direction'],
                    'ExitDate': t['date'],
                    'Exit-Weekday': t['weekday'],
                    'ExitTime': t['exit_time'],
                    'ExitPrice': t['exit'],
                    'P/L': round(t['pnl'], 2),
                    'Brokerage': round(t['brokerage'], 2),
                    'P/L-Percentage': t['pnl_pct'],
                    'ExpiryDate': t['expiry_date'],
                    'Highest MTM(Candle Close)': '',
                    'Lowest MTM(Candle Close)': '',
                    'Remarks[exit reason]': t['exit_reason'],
                    'Strategy': t['strategy'],
                    'Symbol': t['symbol'],
                })

        # Log summary
        logging.info(f'{self.__class__.__name__}: Report saved to {report_file}')
        logging.info(f'{self.__class__.__name__}: Trades appended to {csv_file}')
        logging.info(f'{self.__class__.__name__}: Date: {self.test_date}')
        logging.info(f'{self.__class__.__name__}: Total PnL: {self.backtest_results["total_pnl"]:.2f}')
        logging.info(f'{self.__class__.__name__}: Total Trades: {self.backtest_results["total_trades"]}')

        # Log per-trade details
        for t in self.backtest_results['trades']:
            logging.info(
                f'  {t["symbol"]:30s}  entry={t["entry_time"]}  exit={t["exit_time"]}  '
                f'pnl={t["pnl"]:8.2f}  brokerage={t["brokerage"]:6.2f}  reason={t["exit_reason"]}'
            )

    def executeTrade(self, trade):
        """
        Override executeTrade for backtest order placement.

        Market orders (placeMarketOrder=True):
          - normal mode: fill immediately at requestedEntry (MARKET order type)
          - aggressive_mode: fill at next candle open instead

        SL_LIMIT entry orders (placeMarketOrder=False):
          - If trigger already breached at placement time (NSE would reject), fill
            immediately at current candle open as a MARKET order.
          - Otherwise place as SL_LIMIT (TRIGGER_PENDING); checkAndExecuteSLOrders
            will fill on the next candle when trigger is hit.
        """
        from ordermgmt.OrderInputParams import OrderInputParams
        from models.OrderType import OrderType
        from models.Direction import Direction

        trade.initialStopLoss = trade.stopLoss

        oip = OrderInputParams(trade.tradingSymbol)
        oip.exchange = trade.exchange
        oip.direction = trade.direction
        oip.productType = trade.productType
        oip.qty = trade.qty
        oip.tag = trade.strategy
        if trade.isFutures or trade.isOptions:
            oip.isFnO = True

        if trade.placeMarketOrder:
            if self.aggressive_mode:
                # Worst-case fill: use next candle open
                current_timestamp = self.symbolToCMPMap.get('exchange_timestamp')
                symbol_map = self.timestamp_maps.get(trade.tradingSymbol, {})
                sorted_ts = sorted(symbol_map.keys())
                next_candle = None
                try:
                    idx = sorted_ts.index(current_timestamp)
                    if idx + 1 < len(sorted_ts):
                        next_candle = symbol_map[sorted_ts[idx + 1]]
                except ValueError:
                    pass
                fill_price = next_candle['open'] if next_candle else trade.requestedEntry
            else:
                fill_price = trade.requestedEntry

            oip.orderType = OrderType.MARKET
            oip.price = Utils.roundToNSEPrice(fill_price)
            oip.triggerPrice = oip.price
        else:
            # SL_LIMIT entry: always place as TRIGGER_PENDING and let checkAndExecuteSLOrders fill it.
            trigger_price = Utils.roundToNSEPrice(trade.requestedEntry)
            oip.orderType = OrderType.SL_LIMIT
            oip.triggerPrice = trigger_price
            oip.price = Utils.roundToNSEPrice(
                trigger_price * (1.01 if trade.direction == Direction.LONG else 0.99))

        try:
            placedOrder = self.getOrderManager(self.short_code).placeOrder(oip)
            trade.entryOrder.append(placedOrder)
            self.orders[placedOrder.orderId] = placedOrder
        except Exception as e:
            logging.error(
                '%s: Execute trade failed for tradeID %s: Error => %s', self.__class__.__name__, trade.tradeID, str(e))
            return False

        logging.info(
            '%s: Execute trade successful for %s and entryOrder %s', self.__class__.__name__, trade, trade.entryOrder)
        return True

    def placeTargetOrder(self, trade, isMarketOrder=False, targetPrice=0):
        """Target LIMIT orders are placed as OPEN by BacktestOrderManager and filled
        by checkAndExecuteSLOrders when price is reached. No override needed."""
        super().placeTargetOrder(trade, isMarketOrder=isMarketOrder, targetPrice=targetPrice)

    def squareOffTrade(self, trade, reason=None):
        """
        Override to use candle low (LONG exit = sell) or candle high (SHORT exit = buy)
        as the exit price instead of the candle close (trade.cmp).
        Only applies in aggressive_mode; otherwise delegates to the base implementation.
        """
        if not self.aggressive_mode:
            return super().squareOffTrade(trade, reason)

        from trademgmt.TradeState import TradeState
        from models.Direction import Direction
        from trademgmt.TradeExitReason import TradeExitReason

        if reason is None:
            reason = TradeExitReason.SQUARE_OFF

        if trade is None or trade.tradeState != TradeState.ACTIVE:
            return

        current_timestamp = self.symbolToCMPMap.get('exchange_timestamp')
        candle = self.timestamp_maps.get(trade.tradingSymbol, {}).get(current_timestamp)
        if candle:
            if trade.direction == Direction.LONG:
                # Exiting a long = selling — use candle low (worst realistic exit)
                trade.cmp = candle['low']
            else:
                # Exiting a short = buying — use candle high (worst realistic exit)
                trade.cmp = candle['high']

        super().squareOffTrade(trade, reason)

    def orderListener(self, orderId, data):
        """
        Override order listener for backtesting
        In backtest mode, simulate order fills based on historical prices
        """
        # Simulate order execution
        logging.info(
            f'{self.__class__.__name__}: Simulating order execution for {orderId}')
        # TODO: Implement order fill simulation logic

        super().orderListener(orderId, data)

    def getTradesFilepath(self):
        """
        Override to return backtest-specific trades filepath
        """
        return os.path.join(
            self.backtest_results_dir,
            f'trades_{self.test_date}.json'
        )

    def getStrategiesFilepath(self):
        """
        Override to return backtest-specific strategies filepath
        """
        return os.path.join(
            self.backtest_results_dir,
            f'strategies_{self.test_date}.json'
        )

    def loadAllTradesFromFile(self):
        pass

    def loadAllStrategiesFromFile(self):
        pass

    def saveAllTradesToFile(self):
        pass

    def saveAllStrategiesToFile(self):
        pass

    def setTradeToCompleted(self, trade, exit, exitReason=None):
        """
        Override to track backtest statistics
        """
        super().setTradeToCompleted(trade, exit, exitReason)

        # Deduct brokerage: ₹43.6 flat per leg (entry + exit) + 0.25% of entry notional
        # 0.25% = STT 0.15% (post Apr 1 2026) + NSE txn 0.03553% + GST ~0.06% + misc
        brokerage = 43.6 + (0.0025 * trade.entry * trade.filledQty)
        trade.pnl -= brokerage

        entry_time = datetime.fromtimestamp(trade.startTimestamp).strftime(
            '%H:%M:%S') if trade.startTimestamp else 'N/A'
        exit_time = datetime.fromtimestamp(trade.endTimestamp).strftime(
            '%H:%M:%S') if trade.endTimestamp else 'N/A'

        test_datetime = datetime.strptime(self.test_date, '%Y-%m-%d')
        weekday = test_datetime.strftime('%A')

        # Instrument kind and strike
        if trade.isOptions:
            option_type = Utils.getTypeFromSymbol(trade.tradingSymbol)
            instrument_kind = option_type  # CE / PE
            strike_price = Utils.getStrikeFromSymbol(trade.tradingSymbol)
        elif trade.isFutures:
            instrument_kind = 'Futures'
            strike_price = ''
        else:
            instrument_kind = 'Equity'
            strike_price = ''

        # Expiry date (same resolution logic as addNewTrade)
        expiry_date = ''
        if (trade.isOptions or trade.isFutures) and trade.underLying:
            strategy_instance = self.strategyToInstanceMap.get(trade.strategy)
            if strategy_instance and hasattr(strategy_instance, 'getExpiryDate'):
                expiry_dt = strategy_instance.getExpiryDate(datetimeObj=test_datetime)
                expiry_date = expiry_dt.strftime('%Y-%m-%d')
            elif strategy_instance and hasattr(strategy_instance, 'expiryDay'):
                expiry_dt = Utils.getWeeklyExpiryDayDate(
                    trade.underLying, dateTimeObj=test_datetime, expiryDay=strategy_instance.expiryDay)
                expiry_date = expiry_dt.strftime('%Y-%m-%d')

        # Gross P/L percentage (before brokerage, for comparability)
        if trade.entry and trade.entry != 0:
            direction_sign = 1 if str(trade.direction) == 'LONG' else -1
            pnl_pct = (exit - trade.entry) / trade.entry * 100 * direction_sign
        else:
            pnl_pct = 0.0

        # Update backtest results
        self.backtest_results['trades'].append({
            'symbol': trade.tradingSymbol,
            'strategy': trade.strategy,
            'entry': trade.entry,
            'exit': exit,
            'pnl': trade.pnl,
            'brokerage': round(brokerage, 2),
            'date': self.test_date,
            'weekday': weekday,
            'entry_time': entry_time,
            'exit_time': exit_time,
            'exit_reason': exitReason,
            'qty': trade.filledQty,
            'direction': str(trade.direction),
            'instrument_kind': instrument_kind,
            'strike_price': strike_price,
            'expiry_date': expiry_date,
            'pnl_pct': round(pnl_pct, 2),
        })

        self.backtest_results['total_trades'] += 1
        self.backtest_results['total_pnl'] += trade.pnl

    def getOrderManager(self, short_code):
        """
        Override to return BacktestOrderManager for simulated order execution
        """
        return BacktestOrderManager(short_code)

    def addNewTrade(self, trade):
        """
        Override to load historical data for the symbol when trade is added
        This ensures we have full day data cached before any quote lookups
        """
        from utils.Utils import Utils

        # Call parent to add the trade to avoid dealing with ticker add into registeredSymbols first
        self.registeredSymbols.append(trade.tradingSymbol)
        super().addNewTrade(trade)

        # Load full day historical data for this symbol
        if trade:

            # Get underlying and expiry for options/futures
            underlying = None
            expiry_date = None

            if trade.isOptions or trade.isFutures:
                underlying = trade.underLying  # e.g., 'NIFTY', 'BANKNIFTY'
                strategy_instance = self.strategyToInstanceMap.get(trade.strategy)

                # Get expiry date using strategy's method or fallback to weekly
                if underlying:
                    from datetime import datetime
                    test_datetime = datetime.strptime(self.test_date, '%Y-%m-%d')

                    # Use strategy's own getExpiryDate if available (handles monthly/weekly)
                    if strategy_instance and hasattr(strategy_instance, 'getExpiryDate'):
                        expiry_datetime = strategy_instance.getExpiryDate(datetimeObj=test_datetime)
                        logging.info(f'Using expiry date from strategy {trade.strategy}: {expiry_datetime}')
                    elif strategy_instance and hasattr(strategy_instance, 'expiryDay'):
                        # Fallback to weekly expiry using strategy's expiryDay
                        logging.info(f'Using expiryDay={strategy_instance.expiryDay} from strategy {trade.strategy}')
                        expiry_datetime = Utils.getWeeklyExpiryDayDate(
                            underlying, dateTimeObj=test_datetime, expiryDay=strategy_instance.expiryDay)
                    else:
                        logging.error(
                            f'{self.__class__.__name__}: Strategy {trade.strategy} has no getExpiryDate or expiryDay - skipping data load for {trade.tradingSymbol}')
                        return

                    expiry_date = expiry_datetime.strftime('%Y-%m-%d')
                    logging.info(f'Calculated expiry date for {trade.tradingSymbol}: {expiry_date}')

            # Load full day data into timestamp_maps (from QuestDB cache or API)
            self._loadHistoricalData(
                trade.tradingSymbol,
                isFnO=(trade.isOptions or trade.isFutures),
                exchange=trade.exchange,
                underlying=underlying,
                expiry_date=expiry_date
            )

    def _getBacktestQuote(self, tradingSymbol, isFnO=None, exchange=None, underlying=None, expiryDay=1, expiry=None):
        """
        Get quote from backtest historical data using timestamp_maps for O(1) lookup
        Looks up the candle data for the current exchange_timestamp
        Falls back to symbolToCMPMap (forward-filled price) if not found

        Args:
          tradingSymbol: Symbol to get quote for
          isFnO: Whether it's F&O instrument
          exchange: Exchange name
          underlying: Underlying symbol for options/futures
          expiryDay: Day of week for expiry (0=Monday, 6=Sunday). Used only if expiry not provided.
          expiry: Optional expiry date string (YYYY-MM-DD). If provided, uses this instead of calculating from expiryDay.
        """
        from models.Quote import Quote
        from utils.Utils import Utils

        quote = Quote(tradingSymbol)

        # Get current exchange timestamp
        current_timestamp = self.symbolToCMPMap.get('exchange_timestamp')

        if current_timestamp is None:
            logging.error(
                f'{self.__class__.__name__}: No exchange_timestamp in symbolToCMPMap for quote lookup')
            return quote

        # Use timestamp_maps for O(1) lookup instead of linear search
        if tradingSymbol in self.timestamp_maps:
            timestamp_map = self.timestamp_maps[tradingSymbol]
            if current_timestamp in timestamp_map:
                candle = timestamp_map[current_timestamp]
                # Populate quote from historical candle data
                quote.lastTradedPrice = candle['close']
                quote.open = candle['open']
                quote.high = candle['high']
                quote.low = candle['low']
                quote.close = candle['close']
                quote.volume = candle.get('volume', 0)
                return quote

        # Symbol missing from timestamp_maps entirely, or present but lacks this timestamp
        # Load single candle for current timestamp for price checking before trade placement
        logging.debug(
            f'{self.__class__.__name__}: Loading single candle for {tradingSymbol} at {current_timestamp}')

        # Use provided expiry or calculate from expiryDay
        expiry_date = None
        if underlying and isFnO:
            if expiry:
                expiry_date = expiry
            else:
                from datetime import datetime
                test_datetime = datetime.strptime(self.test_date, '%Y-%m-%d')
                expiry_datetime = Utils.getWeeklyExpiryDayDate(
                    underlying, dateTimeObj=test_datetime, expiryDay=expiryDay)
                expiry_date = expiry_datetime.strftime('%Y-%m-%d')

        candle = self._loadSingleCandle(
            tradingSymbol,
            current_timestamp,
            isFnO=isFnO,
            exchange=exchange,
            underlying=underlying,
            expiry_date=expiry_date
        )

        if candle:
            # Populate quote from the single candle
            quote.lastTradedPrice = candle['close']
            quote.open = candle['open']
            quote.high = candle['high']
            quote.low = candle['low']
            quote.close = candle['close']
            quote.volume = candle.get('volume', 0)
            return quote

        # Fall back to symbolToCMPMap (forward-filled price from last tick)
        # This happens when symbol doesn't have data at this specific timestamp
        if tradingSymbol in self.symbolToCMPMap:
            quote.lastTradedPrice = self.symbolToCMPMap[tradingSymbol]
            logging.debug(
                f'{self.__class__.__name__}: Using forward-filled price for {tradingSymbol}: {quote.lastTradedPrice}')
        else:
            # No data available at all for this symbol
            logging.warning(
                f'{self.__class__.__name__}: No data available for {tradingSymbol} at {current_timestamp}')

        return quote
