import json
import logging
import os
import time
import traceback

from datetime import datetime
from threading import Thread

from config.Config import getBrokerAppConfig, getServerConfig
from core.Controller import Controller
from models.Direction import Direction
from models.OrderStatus import OrderStatus
from models.OrderType import OrderType
from ordermgmt.OrderInputParams import OrderInputParams
from ordermgmt.OrderModifyParams import OrderModifyParams
from broker.zerodha.ZerodhaOrderManager import ZerodhaOrderManager
from broker.zerodha.ZerodhaTicker import ZerodhaTicker

from instruments.Instruments import Instruments

from broker.icici.ICICIOrderManager import ICICIOrderManager
from broker.icici.ICICITicker import ICICITicker

from utils.Utils import Utils

import datetime
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

from trademgmt.TradeEncoder import TradeEncoder
from trademgmt.TradeExitReason import TradeExitReason
from trademgmt.TradeState import TradeState


class TradeManager(Thread):

    def __init__(self, group=None, target=None, name=None, args=(), kwargs=None):
        super(TradeManager, self).__init__(
            group=group, target=target, name=name)
        self._accessToken, self.algoConfig, = args
        self.ticker = None
        self.trades = []  # to store all the trades
        self.orders = {}
        self.strategyToInstanceMap = {}
        self.strategiesData = {}
        self.symbolToCMPMap = {}
        self.intradayTradesDir = None
        self.registeredSymbols = []
        self.isReady = False
        self.is_backtest_mode = False
        self.highestMarginUsed = 0.0
        self.dayHighestPnl = 0.0
        self.dayLowestPnl = 0.0
        self.algoTrailSL = 0.0        # Runtime: current trail SL level (PnL/multiple). Reset each day.
        self.algoTrailPeak = 0.0      # Runtime: PnL/multiple level that last triggered a trail update. Reset each day.
        self.algoTargetActivated = False  # Runtime: True once algoTarget has been hit. Reset each day.
        # Copy static config values once so checkStrategyHealth uses self.* throughout
        self.algoSL = self.algoConfig.algoSL
        self.algoTarget = self.algoConfig.algoTarget
        self.algoTrailOffset = self.algoConfig.algoTrailOffset
        self.algoTrailStep = self.algoConfig.algoTrailStep
        self.multiple = self.algoConfig.multiple
        self.pausedStrategies = set()  # strategy names paused by shadow deviations

        # Initialize Quotes instance (will be connected to QuestDB in run())
        from core.Quotes import Quotes
        self.quotes = Quotes()

    def run(self):

        # check and create trades directory for today`s date
        serverConfig = getServerConfig()
        tradesDir = os.path.join(serverConfig['deployDir'], 'trades')
        self.intradayTradesDir = os.path.join(
            tradesDir, Utils.getTodayDateStr(Utils.getExchangeTimestamp(self.name)))
        if os.path.exists(self.intradayTradesDir) == False:
            logging.info('TradeManager: Intraday Trades Directory %s does not exist. Hence going to create.',
                         self.intradayTradesDir)
            os.makedirs(self.intradayTradesDir)

        instrumentsList = Instruments.fetchInstruments(self.name)    

        if len(instrumentsList) == 0:
            #something is wrong. We need to inform the user
            logging.warning("Algo not started.")
            return

        # start ticker service
        brokerName = getBrokerAppConfig(self.name)['broker']
        if brokerName == "zerodha":
            self.ticker = ZerodhaTicker(self.name)
        elif brokerName == "icici":
            self.ticker = ICICITicker(self.name)
        # elif brokerName == "fyers" # not implemented
        # ticker = FyersTicker()

        self.ticker.startTicker(
            Controller.brokerLogin[self.name].getBrokerAppDetails().appKey, self._accessToken)
        
        self.ticker.registerListener(self.tickerListener)

        self.ticker.registerOrderListener(self.orderListener)

        self.ticker.registerSymbols(["NIFTY 50", "NIFTY BANK", "INDIA VIX", "SENSEX"], mode = "full")

        # Load all trades from json files to app memory
        self.loadAllTradesFromFile()
        self.loadAllStrategiesFromFile()

        # sleep for 2 seconds for ticker to update ltp map
        time.sleep(2)

        self.isReady = True

        Utils.waitTillMarketOpens("TradeManager")

        

        # track and update trades in a loop
        while True:

            if not Utils.isTodayHoliday(Utils.getExchangeTimestamp(self.name)) and not Utils.isMarketClosedForTheDay(Utils.getExchangeTimestamp(self.name)) and not len(self.strategyToInstanceMap) == 0:
                try:
                    # Fetch all order details from broker and update orders in each trade
                    self.fetchAndUpdateAllTradeOrders()
                    # track each trade and take necessary action
                    self.trackAndUpdateAllTrades()

                    # pe_vega = 0
                    # ce_vega = 0
                    # indexSymbol = "NIFTY BANK"
                    # quote = self.symbolToCMPMap.get(indexSymbol, None)
                    # if quote is not None:
                    #     symbolsToTrack = []
                    #     ATMStrike = Utils.getNearestStrikePrice(quote, 100)
                    #     ATMCESymbol = Utils.prepareWeeklyOptionsSymbol("BANKNIFTY", ATMStrike, 'CE')
                    #     ATMPESymbol = Utils.prepareWeeklyOptionsSymbol("BANKNIFTY", ATMStrike, 'PE')
                    #     symbolsToTrack.append(ATMCESymbol)
                    #     symbolsToTrack.append(ATMPESymbol)

                    #     if self.symbolToCMPMap.get(ATMCESymbol, None) is not None:
                    #         greeks = Utils.greeks(self.symbolToCMPMap[ATMCESymbol], Utils.getWeeklyExpiryDayDate(), self.symbolToCMPMap["NIFTY BANK"], ATMStrike, 0.1, "CE")
                    #         ce_vega += greeks['Vega']
                    #     if self.symbolToCMPMap.get(ATMPESymbol, None) is not None:
                    #         greeks = Utils.greeks(self.symbolToCMPMap[ATMPESymbol], Utils.getWeeklyExpiryDayDate(), self.symbolToCMPMap["NIFTY BANK"], ATMStrike, 0.1, "PE")
                    #         pe_vega += greeks['Vega']

                    #     for i in range(10):
                    #         OTMPEStrike = ATMStrike - 100 * i
                    #         OTMCEStrike = ATMStrike + 100 * i
                    #         OTMCESymbol = Utils.prepareWeeklyOptionsSymbol("BANKNIFTY", OTMCEStrike, 'CE')
                    #         OTMPESymbol = Utils.prepareWeeklyOptionsSymbol("BANKNIFTY", OTMPEStrike, 'PE')
                    #         symbolsToTrack.append(OTMCESymbol)
                    #         symbolsToTrack.append(OTMPESymbol)

                    #         if self.symbolToCMPMap.get(OTMCESymbol, None) is not None:
                    #             greeks = Utils.greeks(self.symbolToCMPMap[OTMCESymbol], Utils.getWeeklyExpiryDayDate(), self.symbolToCMPMap["NIFTY BANK"], OTMCEStrike, 0.1, "CE")
                    #             # print("%s : %s" %(OTMCESymbol, greeks))
                    #             ce_vega += greeks['Vega']
                    #         if self.symbolToCMPMap.get(OTMPESymbol, None) is not None:
                    #             greeks = Utils.greeks(self.symbolToCMPMap[OTMPESymbol], Utils.getWeeklyExpiryDayDate(), self.symbolToCMPMap["NIFTY BANK"], OTMPEStrike, 0.1, "PE")
                    #             # print("%s : %s" %(OTMPESymbol, greeks))
                    #             pe_vega += greeks['Vega']
                    #     self.ticker.registerSymbols(symbolsToTrack)

                    self.checkStrategyHealth()
                    self.updateHighestMarginUsed()
                    # print ( "%s =>%f :: %f" %(datetime.now().strftime("%H:%M:%S"), pe_vega, ce_vega))
                except Exception as e:
                    traceback.print_exc()
                    logging.exception("Exception in TradeManager Main thread")

                # save updated data to json file
                self.saveAllTradesToFile()
                self.saveAllStrategiesToFile()

            # Use idle time to cache 1-min candles for active symbols into QuestDB
            now = datetime.now()
            waitSeconds = 5 - (now.second % 5)
            candle_symbols = [
                sym for sym, cmp in self.symbolToCMPMap.items()
                if sym != 'exchange_timestamp' and cmp > 5.0
            ]
            if candle_symbols:
                try:
                    today_str = Utils.getTodayDateStr(Utils.getExchangeTimestamp(self.name))
                    last_closed = datetime.now().replace(second=0, microsecond=0) - timedelta(minutes=1)
                    t_str = last_closed.strftime('%H:%M:%S')
                    for symbol in candle_symbols:
                        self.quotes.getHistoricalData(symbol, self.name, today_str, isFnO=True, exchange='NFO', from_time=t_str, to_time=t_str)
                except Exception:
                    pass

            # Retry pending whisker hit checks (slWhiskerHit is None = candle wasn't ready at exit time)
            for trade in self.trades:
                if trade.slWhiskerHit is None:
                    self._checkWhiskerHit(trade)
            time.sleep(waitSeconds)

    def registerStrategy(self, strategyInstance):
        self.strategyToInstanceMap[strategyInstance.getName()] = strategyInstance
        strategyInstance.strategyData = self.strategiesData.get(
            strategyInstance.getName(), None)

    def startStrategyExecution(self, strategyInstance, threadName):
        """Start a strategy. Live mode runs it in a thread."""
        import threading
        threading.Thread(target=strategyInstance.run, name=threadName).start()

    def squareOffStrategy(self, strategy, reason):
        if reason != TradeExitReason.ALGO_TRAIL_SL_HIT:
            strategy.setDisabled()
        for trade in strategy.trades:
            if trade.tradeState in (TradeState.ACTIVE):
                self.squareOffTrade(trade, reason)

    def deRgisterStrategy(self, strategyInstance):
        del self.strategyToInstanceMap[strategyInstance.getName()]

    def loadAllTradesFromFile(self):
        tradesFilepath = self.getTradesFilepath()
        if os.path.exists(tradesFilepath) == False:
            logging.warning(
                'TradeManager: loadAllTradesFromFile() Trades Filepath %s does not exist', tradesFilepath)
            return
        self.trades = []
        tFile = open(tradesFilepath, 'r')
        tradesData = json.loads(tFile.read())
        for tr in tradesData:
            trade = Utils.convertJSONToTrade(tr)
            logging.info('loadAllTradesFromFile trade => %s', trade)
            self.trades.append(trade)
            if trade.tradingSymbol not in self.registeredSymbols:
                # Algo register symbols with ticker
                self.ticker.registerSymbols([trade.tradingSymbol])
                self.registeredSymbols.append(trade.tradingSymbol)
        logging.info('TradeManager: Successfully loaded %d trades from json file %s', len(
            self.trades), tradesFilepath)
        
    def loadAllStrategiesFromFile(self):
        strategiesFilePath = self.getStrategiesFilepath()
        if os.path.exists(strategiesFilePath) == False:
            logging.warning(
                'TradeManager: loadAllTradesFromFile() Trades Filepath %s does not exist', strategiesFilePath)
            return
        sFile = open(strategiesFilePath, 'r')
        self.strategiesData = json.loads(sFile.read())
        meta = self.strategiesData.pop('_meta', {})
        self.highestMarginUsed = meta.get('highestMarginUsed', 0.0)
        logging.info('TradeManager: Successfully loaded %d strategies from json file %s', len(
            self.strategiesData), strategiesFilePath)



    def getTradesFilepath(self):
        tradesFilepath = os.path.join(self.intradayTradesDir, getBrokerAppConfig(self.name)[
                                      'broker']+'_'+getBrokerAppConfig(self.name)['clientID']+'.json')
        return tradesFilepath

    def getStrategiesFilepath(self):
        tradesFilepath = os.path.join(self.intradayTradesDir, getBrokerAppConfig(self.name)[
                                      'broker']+'_'+getBrokerAppConfig(self.name)['clientID']+'_strategies.json')
        return tradesFilepath

    def saveAllTradesToFile(self):
        tradesFilepath = self.getTradesFilepath()
        with open(tradesFilepath, 'w') as tFile:
            json.dump(self.trades, tFile, indent=2, cls=TradeEncoder)
        logging.info('TradeManager: Saved %d trades to file %s',
                     len(self.trades), tradesFilepath)
    
    def saveAllStrategiesToFile(self):
        strategiesFilePath = self.getStrategiesFilepath()
        data = dict(self.strategyToInstanceMap)
        data['_meta'] = {'highestMarginUsed': self.highestMarginUsed}
        with open(strategiesFilePath, 'w') as tFile:
            json.dump(data, tFile, indent=2, cls=TradeEncoder)
        logging.info('TradeManager: Saved %d strategies to file %s',
                     len(self.strategyToInstanceMap.values()), strategiesFilePath)

    def updateHighestMarginUsed(self):
        try:
            margins = Controller.getBrokerLogin(self.name).getBrokerHandle().margins()
            used = margins['equity']['utilised']['debits'] / 100000
            if used > self.highestMarginUsed:
                self.highestMarginUsed = used
        except Exception:
            logging.warning('TradeManager: Could not fetch margins for highest margin tracking')

    def addNewTrade(self, trade):
        if trade == None:
            return
        logging.info('%s: addNewTrade called for %s', self.name, trade)
        for tr in self.trades:
            if tr.equals(trade):
                logging.warning(
                    '%s: Trade already exists so not adding again. %s', self.name, trade)
                return
        # Add the new trade to the list
        self.trades.append(trade)
        logging.info(
            '%s: trade %s added successfully to the list', self.name, trade.tradeID)
        # Register the symbol with ticker so that we will start getting ticks for this symbol
        if trade.tradingSymbol not in self.registeredSymbols:
            self.ticker.registerSymbols([trade.tradingSymbol])
            self.registeredSymbols.append(trade.tradingSymbol)
        # Also add the trade to strategy trades list
        strategyInstance = self.strategyToInstanceMap[trade.strategy]
        if strategyInstance != None:
            strategyInstance.addTradeToList(trade)

    def disableTrade(self, trade, reason):
        if trade != None:
            logging.info(
                '%s: Going to disable trade ID %s with the reason %s', self.name, trade.tradeID, reason)
            trade.tradeState = TradeState.DISABLED

    def orderListener(self, orderId, data):
        self.getOrderManager(self.name).updateOrder(self.orders.get(orderId, None), data)

    def tickerListener(self, tick):
        # logging.info('tickerLister: new tick received for %s = %f', tick.tradingSymbol, tick.lastTradedPrice);
        # Store the latest tick in map
        self.symbolToCMPMap[tick.tradingSymbol] = tick.lastTradedPrice
        if tick.exchange_timestamp:
            # Store exchange timestamp as-is (timezone-naive)
            exchange_ts = tick.exchange_timestamp
            if hasattr(exchange_ts, 'tzinfo') and exchange_ts.tzinfo:
                exchange_ts = exchange_ts.replace(tzinfo=None)
            self.symbolToCMPMap["exchange_timestamp"] = exchange_ts
        # On each new tick, get a created trade and call its strategy whether to place trade or not
        for strategy in self.strategyToInstanceMap:
            if strategy in self.pausedStrategies:
                longTrade = self.getUntriggeredTrade(
                    tick.tradingSymbol, strategy, Direction.LONG)
                shortTrade = self.getUntriggeredTrade(
                    tick.tradingSymbol, strategy, Direction.SHORT)
                if longTrade is not None or shortTrade is not None:
                    logging.info(
                        '%s: strategy %s is paused, skipping pending trade(s): %s',
                        self.name, strategy,
                        ' '.join([t.tradeID for t in [longTrade, shortTrade] if t is not None])
                    )
                continue
            longTrade = self.getUntriggeredTrade(
                tick.tradingSymbol, strategy, Direction.LONG)
            shortTrade = self.getUntriggeredTrade(
                tick.tradingSymbol, strategy, Direction.SHORT)
            if longTrade == None and shortTrade == None:
                continue
            strategyInstance = self.strategyToInstanceMap[strategy]
            if longTrade != None:
                if strategyInstance.shouldPlaceTrade(longTrade, tick):
                    # place the longTrade
                    isSuccess = self.executeTrade(longTrade)
                    if isSuccess == True:
                        # set longTrade state to ACTIVE
                        longTrade.tradeState = TradeState.ACTIVE
                        longTrade.startTimestamp = Utils.getEpoch(
                            short_code=self.name)
                        continue
                    else:
                        longTrade.tradeState = TradeState.DISABLED

            if shortTrade != None:
                if strategyInstance.shouldPlaceTrade(shortTrade, tick):
                    # place the shortTrade
                    isSuccess = self.executeTrade(shortTrade)
                    if isSuccess == True:
                        # set shortTrade state to ACTIVE
                        shortTrade.tradeState = TradeState.ACTIVE
                        shortTrade.startTimestamp = Utils.getEpoch(
                            short_code=self.name)
                    else:
                        shortTrade.tradeState = TradeState.DISABLED

    def getUntriggeredTrade(self, tradingSymbol, strategy, direction):
        trade = None
        for tr in self.trades:
            if tr.tradeState == TradeState.DISABLED:
                continue
            if tr.tradeState != TradeState.CREATED:
                continue
            if tr.tradingSymbol != tradingSymbol:
                continue
            if tr.strategy != strategy:
                continue
            if tr.direction != direction:
                continue
            trade = tr
            break
        return trade

    def _placeOrders(self, oip, orderList):
        """Slice oip into multiple orders if qty exceeds the broker freeze limit, appending all placed orders to orderList."""
        import math
        orderManager = self.getOrderManager(self.name)
        max_qty = orderManager.getMaxOrderQuantity(oip.tradingSymbol)
        if max_qty and oip.qty > max_qty:
            lot_size = Instruments.getInstrumentDataBySymbol(self.name, oip.tradingSymbol)['lot_size']
            lots_per_slice = max_qty // lot_size
            qty_per_slice = lots_per_slice * lot_size
            remaining = oip.qty
            # Market orders cannot be placed above freeze limit
            if oip.orderType == OrderType.MARKET:
                oip.orderType = OrderType.LIMIT
            while remaining > 0:
                slice_oip = OrderInputParams(oip.tradingSymbol)
                slice_oip.exchange = oip.exchange
                slice_oip.isFnO = oip.isFnO
                slice_oip.productType = oip.productType
                slice_oip.orderType = oip.orderType
                slice_oip.direction = oip.direction
                slice_oip.price = oip.price
                slice_oip.triggerPrice = oip.triggerPrice
                slice_oip.qty = min(qty_per_slice, remaining)
                slice_oip.tag = oip.tag
                placedOrder = orderManager.placeOrder(slice_oip)
                orderList.append(placedOrder)
                self.orders[placedOrder.orderId] = placedOrder
                logging.info('%s: Placed sliced order %s qty=%d (remaining=%d of total=%d)',
                             self.name, placedOrder.orderId, slice_oip.qty, remaining - slice_oip.qty, oip.qty)
                remaining -= qty_per_slice
        else:
            placedOrder = orderManager.placeOrder(oip)
            orderList.append(placedOrder)
            self.orders[placedOrder.orderId] = placedOrder

    def executeTrade(self, trade):
        logging.info('%s: Execute trade called for %s', self.name, trade)
        trade.initialStopLoss = trade.stopLoss
        # Create order input params object and place order
        oip = OrderInputParams(trade.tradingSymbol)
        oip.exchange = trade.exchange
        oip.direction = trade.direction
        oip.productType = trade.productType
        if not trade.placeMarketOrder:
            cmp = self.symbolToCMPMap.get(trade.tradingSymbol)
            trigger = trade.requestedEntry
            # If trigger already breached, SL_LIMIT would be rejected by broker; fall back to LIMIT
            trigger_already_hit = (
                cmp is not None and (
                    (trade.direction == Direction.SHORT and cmp < trigger) or
                    (trade.direction == Direction.LONG and cmp > trigger)
                )
            )
            if trigger_already_hit:
                logging.info(
                    '%s: SL_LIMIT trigger %.2f already breached (CMP=%.2f) for %s, falling back to LIMIT order',
                    self.name, trigger, cmp, trade.tradingSymbol
                )
                oip.orderType = OrderType.LIMIT
                oip.triggerPrice = 0
                oip.price = Utils.roundToNSEPrice(cmp * (1.01 if trade.direction == Direction.LONG else 0.99))
            else:
                oip.orderType = OrderType.SL_LIMIT
                oip.triggerPrice = Utils.roundToNSEPrice(trigger)
                oip.price = Utils.roundToNSEPrice(trigger * (1.01 if trade.direction == Direction.LONG else 0.99))
        else:
            oip.orderType = OrderType.LIMIT
            oip.triggerPrice = Utils.roundToNSEPrice(trade.requestedEntry)
            oip.price = Utils.roundToNSEPrice(trade.requestedEntry *
                                              (1.01 if trade.direction == Direction.LONG else 0.99))
        oip.qty = trade.qty
        oip.tag = trade.strategy
        if trade.isFutures == True or trade.isOptions == True:
            oip.isFnO = True
        try:
            self._placeOrders(oip, trade.entryOrder)
        except Exception as e:
            logging.error(
                '%s: Execute trade failed for tradeID %s: Error => %s', self.name, trade.tradeID, str(e))
            return False

        logging.info(
            '%s: Execute trade successful for %s and entryOrder %s', self.name, trade, trade.entryOrder)
        return True

    def fetchAndUpdateAllTradeOrders(self):
        allOrders = {}
        for trade in self.trades:
            for entryOrder in trade.entryOrder:
                allOrders[entryOrder] = trade.strategy
            for slOrder in trade.slOrder:
                allOrders[slOrder] = trade.strategy
            for targetOrder in trade.targetOrder:
                allOrders[targetOrder] = trade.strategy

        missingOrders = self.getOrderManager(
            self.name).fetchAndUpdateAllOrderDetails(allOrders)
        
        #lets find the place for these orders
        for missingOrder in missingOrders: 
            orderParentFound = False     
            for trade in self.trades:
                for entryOrder in trade.entryOrder:
                    if entryOrder.orderId == missingOrder.parentOrderId:
                        trade.entryOrder.append(missingOrder)
                        self.orders[missingOrder.orderId] = missingOrder
                        orderParentFound = True
                for slOrder in trade.slOrder:
                    if slOrder.orderId == missingOrder.parentOrderId:
                        trade.slOrder.append(missingOrder)
                        self.orders[missingOrder.orderId] = missingOrder
                        orderParentFound = True
                for targetOrder in trade.targetOrder:
                    if targetOrder.orderId == missingOrder.parentOrderId:
                        trade.targetOrder.append(missingOrder)
                        self.orders[missingOrder.orderId] = missingOrder
                        orderParentFound = True
                if orderParentFound:
                    break

    def _refreshActiveTradePnl(self):
        for trade in self.trades:
            if trade.tradeState == TradeState.ACTIVE:
                trade.cmp = self.symbolToCMPMap.get(trade.tradingSymbol, trade.cmp)
                Utils.calculateTradePnl(trade)

    def trackAndUpdateAllTrades(self):

        if not self.is_backtest_mode:
            try:
                with Utils._getQuestDBCursor() as cursor:
                    query = "INSERT INTO '{0}' VALUES('{1}', '{2}', '{3}', '{4}', {5}, {6}, {7}, {8}, '{9}');"\
                        .format(self.name, datetime.now(), "Nifty", "NIFTY 50", "",
                                self.getLastTradedPrice("NIFTY 50"), 0, 0, 0, "")
                    cursor.execute(query)
                    query = "INSERT INTO '{0}' VALUES('{1}', '{2}', '{3}', '{4}', {5}, {6}, {7}, {8}, '{9}');"\
                        .format(self.name, datetime.now(), "BankNifty", "NIFTY BANK", "",
                                self.getLastTradedPrice("NIFTY BANK"), 0, 0, 0, "")
                    cursor.execute(query)
                    query = "INSERT INTO '{0}' VALUES('{1}', '{2}', '{3}', '{4}', {5}, {6}, {7}, {8}, '{9}');"\
                        .format(self.name, datetime.now(), "VIX", "INDIA VIX", "",
                                self.getLastTradedPrice("INDIA VIX"), 0, 0, 0, "")
                    cursor.execute(query)
            except Exception as err:
                logging.error("Error inserting into Quest DB %s", str(err))

        for trade in self.trades:
            if trade.tradeState == TradeState.ACTIVE:
                self.trackEntryOrder(trade)
                self.trackTargetOrder(trade)
                self.trackSLOrder(trade)
                if trade.intradaySquareOffTimestamp != None:
                    nowEpoch = Utils.getEpoch(short_code=self.name)
                    if nowEpoch >= trade.intradaySquareOffTimestamp:
                        logging.info(
                            '%s: Square-off time reached for trade %s. nowEpoch=%d, squareOffEpoch=%d, symbol=%s, target=%.2f',
                            self.name, trade.tradeID, nowEpoch, trade.intradaySquareOffTimestamp,
                            trade.tradingSymbol, self.symbolToCMPMap[trade.tradingSymbol]
                        )
                        trade.target = self.symbolToCMPMap[trade.tradingSymbol]
                        self.squareOffTrade(
                            trade, TradeExitReason.SQUARE_OFF)

    def checkStrategyHealth(self):
        totalPnl = sum(trade.pnl for trade in self.trades)
        if totalPnl > self.dayHighestPnl:
            self.dayHighestPnl = totalPnl
        if totalPnl < self.dayLowestPnl:
            self.dayLowestPnl = totalPnl

        if self.algoSL < 0 and totalPnl < self.algoSL:
            logging.warning("Algo SL hit: totalPnl=%.0f < algoSL=%.0f. Squaring off all strategies.", totalPnl, self.algoSL)
            self._squareOffAllStrategies(TradeExitReason.ALGO_SL_HIT)
            return

        if self.algoTarget != 0 or self.algoTrailSL != 0 or self.algoTrailOffset != 0:
            pnlPerMultiple = totalPnl / self.multiple

            if self.algoTrailSL > 0 and pnlPerMultiple < self.algoTrailSL:
                logging.warning("Algo trailing SL hit: PnL/multiple=%.0f < algoTrailSL=%.0f. Squaring off all strategies.", pnlPerMultiple, self.algoTrailSL)
                self._squareOffAllStrategies(TradeExitReason.ALGO_TRAIL_SL_HIT)
                return

            if self.algoTrailOffset > 0:
                if not self.algoTargetActivated and self.algoTarget > 0 and pnlPerMultiple >= self.algoTarget:
                    self.algoTargetActivated = True
                    self.algoTrailSL = pnlPerMultiple - self.algoTrailOffset
                    self.algoTrailPeak = pnlPerMultiple
                    logging.warning("Algo target hit: PnL/multiple=%.0f >= algoTarget=%.0f. Trail SL set to %.0f/multiple.", pnlPerMultiple, self.algoTarget, self.algoTrailSL)
                elif self.algoTrailSL > 0 and pnlPerMultiple >= self.algoTrailPeak + self.algoTrailStep:
                    self.algoTrailSL = pnlPerMultiple - self.algoTrailOffset
                    self.algoTrailPeak = pnlPerMultiple
                    logging.warning("Algo trail SL updated to %.0f/multiple (PnL/multiple=%.0f).", self.algoTrailSL, pnlPerMultiple)

        for strategy in self.strategyToInstanceMap.values():
            if strategy.isEnabled():
                SLorTargetHit = strategy.isTargetORSLHit()
                if(SLorTargetHit is not None):
                    self.squareOffStrategy(strategy, SLorTargetHit)

    def _squareOffAllStrategies(self, reason):
        if reason == TradeExitReason.ALGO_TRAIL_SL_HIT:
            self.algoTrailSL = 0
            self.algoTrailPeak = 0
            self.algoTargetActivated = False
        for strategy in list(self.strategyToInstanceMap.values()):
            if strategy.isEnabled():
                self.squareOffStrategy(strategy, reason)

    def trackEntryOrder(self, trade):
        if trade.tradeState != TradeState.ACTIVE:
            return

        if len(trade.entryOrder) == 0:
            return

        trade.filledQty = 0
        trade.entry = 0
        orderCanceled = 0
        orderRejected = 0

        for entryOrder in trade.entryOrder:
            if entryOrder.orderStatus == OrderStatus.CANCELLED:
                orderCanceled += 1

            if entryOrder.orderStatus ==  entryOrder.orderStatus == OrderStatus.REJECTED:
                orderRejected +=1

            if entryOrder.filledQty > 0:
                trade.entry = (trade.entry * trade.filledQty + entryOrder.averagePrice *
                               entryOrder.filledQty) / (trade.filledQty+entryOrder.filledQty)
            elif entryOrder.orderStatus not in [OrderStatus.REJECTED, OrderStatus.CANCELLED, None] and not entryOrder.orderType in [OrderType.SL_LIMIT]:
                omp = OrderModifyParams()
                newPrice = (entryOrder.price + self.symbolToCMPMap[trade.tradingSymbol])*0.5
                if trade.direction == Direction.LONG:
                    omp.newPrice = Utils.roundToNSEPrice(newPrice * 1.01) + 0.05
                else:
                    omp.newPrice = Utils.roundToNSEPrice(newPrice * 0.99) - 0.05
                try:  
                    self.getOrderManager(self.name).modifyOrder(
                        entryOrder, omp, trade.qty)
                except Exception as e:
                    if e.args[0] == "Maximum allowed order modifications exceeded.":
                        self.getOrderManager(self.name).cancelOrder(entryOrder)
            elif entryOrder.orderStatus in [OrderStatus.TRIGGER_PENDING]:
                nowEpoch = Utils.getEpoch(short_code=self.name)
                if nowEpoch >= Utils.getEpoch(self.strategyToInstanceMap[trade.strategy].stopTimestamp):
                    self.getOrderManager(self.name).cancelOrder(entryOrder)

            trade.filledQty += entryOrder.filledQty

        if orderCanceled == len(trade.entryOrder):
            trade.tradeState = TradeState.CANCELLED
        
        if orderRejected == len(trade.entryOrder):
            trade.tradeState = TradeState.DISABLED  
        
        if orderRejected > 0:
            strategy = self.strategyToInstanceMap[trade.strategy]
            for trade in strategy.trades:
                if trade.tradeState in (TradeState.ACTIVE):
                    trade.target = self.symbolToCMPMap[trade.tradingSymbol]
                    self.squareOffTrade(trade, TradeExitReason.TRADE_FAILED)
                strategy.setDisabled()

        # Update the current market price and calculate pnl
        trade.cmp = self.symbolToCMPMap[trade.tradingSymbol]
        Utils.calculateTradePnl(trade)

        if not self.is_backtest_mode:
            try:
                with Utils._getQuestDBCursor() as cursor:
                    query = "INSERT INTO '{0}' VALUES('{1}', '{2}', '{3}', '{4}', {5}, {6}, {7}, {8}, '{9}');"\
                        .format(self.name, datetime.now(), trade.strategy, trade.tradingSymbol, trade.tradeID,
                                trade.cmp, trade.entry, trade.pnl, trade.qty, trade.tradeState)
                    cursor.execute(query)
            except Exception as err:
                logging.error("Error inserting into Quest DB %s", str(err))

    def trackSLOrder(self, trade):
        for entryOrder in trade.entryOrder:
            if entryOrder.orderStatus in [OrderStatus.OPEN, OrderStatus.TRIGGER_PENDING]:
                return  # wait until all entry slices are filled before placing SL
        if trade.stopLoss == 0 and trade.entry > 0:
            # check if stoploss is yet to be calculated
            newSL = self.strategyToInstanceMap.get(
                trade.strategy, None).getTrailingSL(trade)
            if newSL == 0:
                return
            else:
                trade.stopLoss = newSL

        if len(trade.slOrder) == 0 and trade.entry > 0:
            # Place SL order
            self.placeSLOrder(trade)
        else:
            slCompleted = 0
            slAverage = 0
            slQuantity = 0
            slCancelled = 0
            slRejected = 0
            slOpen = 0
            for slOrder in trade.slOrder:
                if slOrder.orderStatus == OrderStatus.COMPLETE:
                    slCompleted+=1
                    slAverage = (slQuantity * slAverage + slOrder.filledQty * slOrder.averagePrice) / (slQuantity+slOrder.filledQty)
                    slQuantity  += slOrder.filledQty
                elif slOrder.orderStatus == OrderStatus.CANCELLED:
                    slCancelled+=1
                elif slOrder.orderStatus == OrderStatus.REJECTED:
                    slRejected+=1
                elif slOrder.orderStatus == OrderStatus.OPEN:
                    slOpen+=1
                    
                    omp = OrderModifyParams()
                    if trade.direction == Direction.LONG:
                        newPrice = (slOrder.price + self.symbolToCMPMap[trade.tradingSymbol])*0.4
                        omp.newTriggerPrice = Utils.roundToNSEPrice(newPrice) - 0.05
                        omp.newPrice = Utils.roundToNSEPrice(newPrice * 0.99) - 0.05
                    else:
                        newPrice = (slOrder.price + self.symbolToCMPMap[trade.tradingSymbol])*0.6
                        omp.newTriggerPrice = Utils.roundToNSEPrice(newPrice) + 0.05
                        omp.newPrice = Utils.roundToNSEPrice(newPrice * 1.01) + 0.05
                        
                        
                    self.getOrderManager(self.name).modifyOrder(
                        slOrder, omp, trade.qty)

            if  slCompleted == len(trade.slOrder) and len(trade.slOrder) > 0 :
                # SL Hit
                exit = slAverage
                exitReason = TradeExitReason.SL_HIT if trade.initialStopLoss == trade.stopLoss else TradeExitReason.TRAIL_SL_HIT
                self.setTradeToCompleted(trade, exit, exitReason)
                # Make sure to cancel target order if exists
                self.cancelTargetOrder(trade)

            elif slCancelled ==  len(trade.slOrder) and len(trade.slOrder) > 0 :
                targetOrderPendingCount  = 0
                for targetOrder in trade.targetOrder:
                    if targetOrder.orderStatus not in [OrderStatus.COMPLETE, OrderStatus.OPEN]:
                        targetOrderPendingCount+=1
                if targetOrderPendingCount == len (trade.targetOrder):
                    # Cancel target order if exists
                    self.cancelTargetOrder(trade)
                    # SL order cancelled outside of algo (manually or by broker or by exchange)
                    logging.error('SL order tradeID %s cancelled outside of Algo. Setting the trade as completed with exit price as current market price.',
                                trade.tradeID)
                    exit = self.symbolToCMPMap[trade.tradingSymbol]
                    self.setTradeToCompleted(
                        trade, exit, TradeExitReason.SL_CANCELLED)
            elif slRejected > 0:
                strategy = self.strategyToInstanceMap[trade.strategy]
                for trade in strategy.trades:
                    if trade.tradeState in (TradeState.ACTIVE):
                        trade.target = self.symbolToCMPMap[trade.tradingSymbol]
                        self.squareOffTrade(trade, TradeExitReason.TRADE_FAILED)
                    strategy.setDisabled()
            elif slOpen > 0 :
                pass #handled above, skip calling trail SL
            else:
                self.checkAndUpdateTrailSL(trade)

    def checkAndUpdateTrailSL(self, trade):
        # Trail the SL if applicable for the trade
        strategyInstance = self.strategyToInstanceMap.get(
            trade.strategy, None)
        if strategyInstance == None:
            return

        newTrailSL = Utils.roundToNSEPrice(
            strategyInstance.getTrailingSL(trade))
        updateSL = False
        if newTrailSL > 0:
            if trade.direction == Direction.LONG and newTrailSL > trade.stopLoss:
                if newTrailSL < trade.cmp:
                    updateSL = True
                else:
                    logging.info(
                        '%s: Trail SL %f triggered Squareoff at market for tradeID %s', self.name, newTrailSL, trade.tradeID)
                    self.squareOffTrade(trade, reason=TradeExitReason.SL_HIT)
            elif trade.direction == Direction.SHORT and newTrailSL < trade.stopLoss:
                if newTrailSL > trade.cmp:
                    updateSL = True
                else:  # in case the SL is called due to all leg squareoff
                    logging.info(
                        '%s: Trail SL %f triggered Squareoff at market for tradeID %s', self.name, newTrailSL, trade.tradeID)
                    self.squareOffTrade(trade, reason=TradeExitReason.SL_HIT)
        if updateSL == True:
            omp = OrderModifyParams()
            omp.newTriggerPrice = newTrailSL
            omp.newPrice = Utils.roundToNSEPrice(
                omp.newTriggerPrice * (0.99 if trade.direction == Direction.LONG else 1.01))  # sl order direction is reverse
            try:
                oldSL = trade.stopLoss
                for slOrder in trade.slOrder:
                    self.getOrderManager(self.name).modifyOrder(
                        slOrder, omp, trade.qty)
                logging.info('%s: Trail SL: Successfully modified stopLoss from %f to %f for tradeID %s', self.name,
                                oldSL, newTrailSL, trade.tradeID)
                # IMPORTANT: Dont forget to update this on successful modification
                trade.stopLoss = newTrailSL
            except Exception as e:
                logging.error('%s: Failed to modify SL order for tradeID %s : Error => %s', self.name,
                              trade.tradeID, str(e))

    def trackTargetOrder(self, trade):
        if trade.tradeState != TradeState.ACTIVE and self.strategyToInstanceMap[trade.strategy].isTargetORSLHit() is not None:
            return
        if trade.target == 0:  # Do not place Target order if no target provided
            return
        if len(trade.targetOrder) == 0 and trade.entry > 0 : #place target order only after the entry happened
            # Place Target order
            logging.info('%s: trackTargetOrder placing target order for tradeID %s, symbol=%s, target=%.2f', self.name, trade.tradeID, trade.tradingSymbol, trade.target)
            self.placeTargetOrder(trade)
        else:
            targetCompleted = 0
            targetAverage = 0
            targetQuantity = 0
            targetCancelled = 0
            targetOpen = 0
            for targetOrder in trade.targetOrder:
                if targetOrder.orderStatus == OrderStatus.COMPLETE:
                    targetCompleted+=1
                    targetAverage = (targetQuantity * targetAverage + targetOrder.filledQty * targetOrder.averagePrice) / (targetQuantity+targetOrder.filledQty)
                    targetQuantity  += targetOrder.filledQty
                elif targetOrder.orderStatus == OrderStatus.CANCELLED:
                    targetCancelled+=1
                elif targetOrder.orderStatus == OrderStatus.OPEN and trade.exitReason is not None:
                    targetOpen+=1
                    omp = OrderModifyParams()
                    newPrice = (targetOrder.price + self.symbolToCMPMap[trade.tradingSymbol])*0.5
                    if trade.direction == Direction.LONG:
                        omp.newTriggerPrice = Utils.roundToNSEPrice(newPrice) - 0.05
                        omp.newPrice = Utils.roundToNSEPrice(newPrice * 0.99) - 0.05
                    else:
                        omp.newTriggerPrice = Utils.roundToNSEPrice(newPrice) + 0.05
                        omp.newPrice = Utils.roundToNSEPrice(newPrice * 1.01) + 0.05
                        
                    self.getOrderManager(self.name).modifyOrder(
                        targetOrder, omp, trade.qty)

            if targetCompleted == len(trade.targetOrder) and len(trade.targetOrder) > 0 :
                # Target Hit
                exit = targetAverage
                self.setTradeToCompleted(
                    trade, exit, TradeExitReason.TARGET_HIT)
                # Make sure to cancel sl order
                self.cancelSLOrder(trade)

            elif targetCancelled == len(trade.targetOrder) and len(trade.targetOrder) > 0 :
                # Target order cancelled outside of algo (manually or by broker or by exchange)
                logging.error('Target orderfor tradeID %s cancelled outside of Algo. Setting the trade as completed with exit price as current market price.',
                               trade.tradeID)
                exit = self.symbolToCMPMap[trade.tradingSymbol]
                self.setTradeToCompleted(
                    trade, exit, TradeExitReason.TARGET_CANCELLED)
                # Cancel SL order
                self.cancelSLOrder(trade)

    def placeSLOrder(self, trade):
        oip = OrderInputParams(trade.tradingSymbol)
        oip.exchange = trade.exchange
        oip.direction = Direction.SHORT if trade.direction == Direction.LONG else Direction.LONG
        oip.productType = trade.productType
        oip.orderType = OrderType.SL_LIMIT
        oip.triggerPrice = Utils.roundToNSEPrice(trade.stopLoss)
        oip.price = Utils.roundToNSEPrice(trade.stopLoss *
                                          (0.99 if trade.direction == Direction.LONG else 1.01))
        oip.qty = trade.qty
        oip.tag = trade.strategy
        if trade.isFutures == True or trade.isOptions == True:
            oip.isFnO = True
        try:
            self._placeOrders(oip, trade.slOrder)
        except Exception as e:
            logging.error(
                '%s: Failed to place SL order for tradeID %s: Error => %s', self.name, trade.tradeID, str(e))
            raise(e)
        logging.info('%s: Successfully placed SL order %s for tradeID %s',
                     self.name, trade.slOrder[0].orderId, trade.tradeID)

    def placeTargetOrder(self, trade, isMarketOrder=False, targetPrice = 0):
        oip = OrderInputParams(trade.tradingSymbol)
        oip.exchange = trade.exchange
        oip.direction = Direction.SHORT if trade.direction == Direction.LONG else Direction.LONG
        oip.productType = trade.productType
        # oip.orderType = OrderType.LIMIT if (
        #     trade.placeMarketOrder == True or isMarketOrder) else OrderType.SL_LIMIT
        oip.orderType = OrderType.MARKET if isMarketOrder == True else OrderType.LIMIT
        if targetPrice == 0:
            targetPrice = trade.target
        oip.triggerPrice = Utils.roundToNSEPrice(targetPrice)
        oip.price = Utils.roundToNSEPrice(targetPrice *
                                          (+ 1.01 if trade.direction == Direction.LONG else 0.99))
        oip.qty = trade.filledQty
        oip.tag = trade.strategy
        if trade.isFutures == True or trade.isOptions == True:
            oip.isFnO = True
        try:
            self._placeOrders(oip, trade.targetOrder)
            trade.target = targetPrice
        except Exception as e:
            logging.error(
                '%s: Failed to place Target order for tradeID %s: Error => %s', self.name, trade.tradeID, str(e))
            raise(e)
        logging.info('%s: Successfully placed Target order %s for tradeID %s',
                     self.name, trade.targetOrder[0].orderId, trade.tradeID)

    def cancelEntryOrder(self, trade):
        if len(trade.entryOrder) == 0:
            return
        for entryOrder in trade.entryOrder:
            if entryOrder.orderStatus == OrderStatus.CANCELLED:
                continue
            try:
                self.getOrderManager(self.name).cancelOrder(entryOrder)
            except Exception as e:
                logging.error('%s: Failed to cancel Entry order %s for tradeID %s: Error => %s', self.name,
                              entryOrder.orderId, trade.tradeID, str(e))
                raise(e)
            logging.info('%s: Successfully cancelled Entry order %s for tradeID %s', self.name,
                         entryOrder.orderId, trade.tradeID)

    def cancelSLOrder(self, trade):
        if len(trade.slOrder) == 0:
            return
        for slOrder in trade.slOrder:
            if slOrder.orderStatus == OrderStatus.CANCELLED:
                continue
            try:
                self.getOrderManager(self.name).cancelOrder(slOrder)
            except Exception as e:
                logging.error('%s: Failed to cancel SL order %s for tradeID %s: Error => %s', self.name,
                              slOrder.orderId, trade.tradeID, str(e))
                raise(e)
            logging.info('%s: Successfully cancelled SL order %s for tradeID %s', self.name,
                         slOrder.orderId, trade.tradeID)

    def cancelTargetOrder(self, trade):
        if len(trade.targetOrder) == 0:
            return
        for targetOrder in trade.targetOrder:
            if targetOrder.orderStatus == OrderStatus.CANCELLED:
                continue
            try:
                self.getOrderManager(self.name).cancelOrder(targetOrder)
            except Exception as e:
                logging.error('%s: Failed to cancel Target order %s for tradeID %s: Error => %s', self.name,
                              targetOrder.orderId, trade.tradeID, str(e))
                raise(e)
            logging.info('%s: Successfully cancelled Target order %s for tradeID %s', self.name,
                         targetOrder.orderId, trade.tradeID)

    def setTradeToCompleted(self, trade, exit, exitReason=None):
        trade.tradeState = TradeState.COMPLETED
        trade.exit = exit
        trade.exitReason = exitReason if trade.exitReason == None else trade.exitReason
        #TODO Timestamp to be matched with last order
        # if trade.targetOrder != None and trade.targetOrder.orderStatus == OrderStatus.COMPLETE:
        #     trade.endTimestamp = datetime.strptime(
        #         trade.targetOrder.lastOrderUpdateTimestamp, "%Y-%m-%d %H:%M:%S").timestamp()
        # elif trade.slOrder != None and trade.slOrder.orderStatus == OrderStatus.COMPLETE:
        #     trade.endTimestamp = datetime.strptime(
        #         trade.slOrder.lastOrderUpdateTimestamp, "%Y-%m-%d %H:%M:%S").timestamp()
        # else:
        trade.endTimestamp = Utils.getEpoch(short_code=self.name)

        trade = Utils.calculateTradePnl(trade)

        from datetime import datetime

        if not self.is_backtest_mode:
            try:
                with Utils._getQuestDBCursor() as cursor:
                    query = "INSERT INTO '{0}' VALUES('{1}', '{2}', '{3}', '{4}', {5}, {6}, {7}, {8}, '{9}');"\
                        .format(self.name, datetime.now(), trade.strategy, trade.tradingSymbol, trade.tradeID,
                                trade.cmp, trade.entry, trade.pnl, trade.qty, trade.tradeState)
                    cursor.execute(query)
            except Exception as err:
                logging.error("Error inserting into Quest DB %s", str(err))

        # Format endTimestamp to human readable format

        start_time_str = datetime.fromtimestamp(trade.startTimestamp).strftime('%Y-%m-%d %H:%M:%S') if trade.startTimestamp else 'N/A'
        end_time_str = datetime.fromtimestamp(trade.endTimestamp).strftime('%Y-%m-%d %H:%M:%S') if trade.endTimestamp else 'N/A'

        logging.info('%s: setTradeToCompleted strategy = %s, symbol = %s, qty = %d, entry = %f, exit = %f, pnl = %f, exit reason = %s, startTimestamp = %s, endTimestamp = %s',
                     self.name, trade.strategy, trade.tradingSymbol, trade.filledQty, trade.entry, trade.exit, trade.pnl, trade.exitReason, start_time_str, end_time_str)

        if not self.is_backtest_mode and trade.exitReason == TradeExitReason.SL_HIT and trade.stopLoss > 0:
            self._checkWhiskerHit(trade)

    def _checkWhiskerHit(self, trade):
        """Check if trade SL was a tick-only hit not confirmed by the 1-min candle.
        Sets trade.slWhiskerHit = True/False if candle is available, None if candle not yet closed."""
        try:
            exit_dt = datetime.fromtimestamp(trade.endTimestamp)
            candle_minute = exit_dt.replace(second=0, microsecond=0)
            # Candle not closed yet — mark pending
            if candle_minute + timedelta(minutes=1) > datetime.now():
                trade.slWhiskerHit = None
                return
            date_str = exit_dt.strftime('%Y-%m-%d')
            t_str = candle_minute.strftime('%H:%M:%S')
            candles = self.quotes.getHistoricalData(
                trade.tradingSymbol, self.name, date_str,
                isFnO=True, exchange='NFO', from_time=t_str, to_time=t_str)
            if not candles:
                trade.slWhiskerHit = None
                return
            candle = candles[-1]
            is_ce = trade.tradingSymbol.endswith('CE')
            if is_ce and candle['high'] < trade.stopLoss:
                trade.slWhiskerHit = True
                logging.warning('[WHISKER HIT] %s SL trigger=%.2f candle_high=%.2f at %s — tick fired, candle did not confirm',
                                trade.tradingSymbol, trade.stopLoss, candle['high'], t_str)
            elif not is_ce and candle['low'] > trade.stopLoss:
                trade.slWhiskerHit = True
                logging.warning('[WHISKER HIT] %s SL trigger=%.2f candle_low=%.2f at %s — tick fired, candle did not confirm',
                                trade.tradingSymbol, trade.stopLoss, candle['low'], t_str)
            else:
                trade.slWhiskerHit = False
        except Exception:
            pass

    def squareOffTrade(self, trade, reason=TradeExitReason.SQUARE_OFF):
        logging.info(
            '%s: squareOffTrade called for tradeID %s with reason %s', self.name, trade.tradeID, reason)
        if trade == None or trade.tradeState != TradeState.ACTIVE:
            return
        
        if trade.exitReason is not None:
            logging.info(
            '%s: squareOffTrade already in progress for tradeID %s with reason %s', self.name, trade.tradeID, trade.exitReason)
            return

        trade.exitReason = reason
        if len(trade.entryOrder) > 0:  
            for entryOrder in trade.entryOrder:
                if entryOrder.orderStatus in [OrderStatus.OPEN, OrderStatus.TRIGGER_PENDING]:
                    # Cancel entry order if it is still open (not filled or partially filled case)
                    self.cancelEntryOrder(trade)
                    break

        if len(trade.slOrder) > 0:
            try:
                self.cancelSLOrder(trade)
            except Exception:
                #probably the order is being processed.
                logging.info('%s: squareOffTrade couldn\'t cancel SL order for %s, not placing target order, strategy will be disabled', self.name,
                         trade.tradeID)
                return


        if len(trade.targetOrder) > 0:
            # Change target order type to MARKET to exit position immediately
            logging.info('%s: changing target order to closer to MARKET to exit tradeID %s', self.name,
                         trade.tradeID)
            for targetOrder in trade.targetOrder:
                if targetOrder.orderStatus == OrderStatus.OPEN:
                    omp = OrderModifyParams()
                    omp.newPrice = Utils.roundToNSEPrice(
                        trade.cmp * (0.99 if trade.direction == Direction.LONG else 1.01))
                    self.getOrderManager(self.name).modifyOrder(
                        targetOrder, omp, trade.filledQty)
        elif trade.entry > 0:
            # Place new target order to exit position, adjust target to current market price
            logging.info(
                '%s: placing new target order to exit position for tradeID %s', self.name, trade.tradeID)
            self.placeTargetOrder(trade, True, targetPrice=(trade.cmp * (0.99 if trade.direction == Direction.LONG else 1.01)))

    def getOrderManager(self, short_code):
        orderManager = None
        brokerName = getBrokerAppConfig(short_code)['broker']
        if brokerName == "zerodha":
            orderManager = ZerodhaOrderManager(Controller.getBrokerLogin(
                short_code).getBrokerHandle(), short_code)
        if brokerName == "icici":
            orderManager = ICICIOrderManager(Controller.getBrokerLogin(
                short_code).getBrokerHandle(), short_code)
        # elif brokerName == "fyers": # Not implemented
        return orderManager

    def getNumberOfTradesPlacedByStrategy(self, strategy):
        count = 0
        for trade in self.trades:
            if trade.strategy != strategy:
                continue
            if trade.tradeState == TradeState.CREATED or trade.tradeState == TradeState.DISABLED:
                continue
            # consider active/completed/cancelled trades as trades placed
            count += 1
        return count

    def getAllTradesByStrategy(self, strategy):
        tradesByStrategy = []
        for trade in self.trades:
            if trade.strategy == strategy:
                tradesByStrategy.append(trade)
        return tradesByStrategy

    def getLastTradedPrice(self, tradingSymbol):
        return self.symbolToCMPMap[tradingSymbol]
    
    def registerTradingSymbolToTrack(self, tradingSymbolsList):
        for tradingSymbol in tradingSymbolsList:
            try:
                if tradingSymbol not in self.registeredSymbols:
                    self.ticker.registerSymbols([tradingSymbol])
                    self.registeredSymbols.append(tradingSymbol)
            except Exception as e:
                logging.error("Error in registerTradingSymbolToTrack for symbol %s,  Error => %s", tradingSymbol, str(e))


