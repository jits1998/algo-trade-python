import logging
import time
from datetime import datetime
from math import ceil

from core.Quotes import Quotes
from instruments.Instruments import Instruments
from models.ProductType import ProductType
from models.Quote import Quote
from trademgmt.Trade import Trade
from trademgmt.TradeExitReason import TradeExitReason
from utils.Utils import Utils


class ZeroPriceError(Exception):
    pass


class BaseStrategy:
    SYMBOL_CONFIG = {
        "NIFTY": {
            "indexSymbol": "NIFTY 50",
            "symbolStrikeInterval": 50,
            "expiryDay": 1,  # Tuesday (weekly)
            "exchange": "NFO",
            "equityExchange": "NSE",
            "expiryType": "weekly",
            "lot_size": 65,
        },
        "SENSEX": {
            "indexSymbol": "SENSEX",
            "symbolStrikeInterval": 100,
            "expiryDay": 3,  # Thursday (weekly)
            "exchange": "BFO",
            "equityExchange": "BSE",
            "expiryType": "weekly",
            "lot_size": 20,
        },
        "BANKNIFTY": {
            "indexSymbol": "NIFTY BANK",
            "symbolStrikeInterval": 100,
            "expiryDay": 1,  # last Tuesday of the month (monthly)
            "exchange": "NFO",
            "equityExchange": "NSE",
            "expiryType": "monthly",
            "lot_size": 30,
        },
        "FINNIFTY": {
            "indexSymbol": "NIFTY FIN SERVICE",
            "symbolStrikeInterval": 100,
            "expiryDay": 1,  # last Tuesday of the month (monthly)
            "exchange": "NFO",
            "equityExchange": "NSE",
            "expiryType": "monthly",
            "lot_size": 60,
        },
        "MIDCPNIFTY": {
            "indexSymbol": "NIFTY MIDCAP 100",
            "symbolStrikeInterval": 50,
            "expiryDay": 1,  # last Tuesday of the month (monthly)
            "exchange": "NFONSE",
            "equityExchange": "NSE",
            "expiryType": "monthly",
            "lot_size": 120,
        },
        "BANKEX": {
            "indexSymbol": "BANKEX",
            "symbolStrikeInterval": 100,
            "expiryDay": 3,  # last Thursday of the month (monthly)
            "exchange": "BFO",
            "equityExchange": "BSE",
            "expiryType": "monthly",
            "lot_size": 30,
        },
    }

    @staticmethod
    def getSymbolConfig(symbol):
        """Return config dict for *symbol*.

        Keys: indexSymbol, symbolStrikeInterval, expiryDay, exchange, equityExchange, expiryType.
        Raises ValueError for unknown symbols.
        """
        if symbol not in BaseStrategy.SYMBOL_CONFIG:
            raise ValueError(f"Unknown symbol: {symbol}")
        return BaseStrategy.SYMBOL_CONFIG[symbol]

    skip_in_backtest = False  # Set True on strategies that should not run in BacktestManager
    skip_in_shadow = False    # Set True on strategies that should not run in ShadowManager

    def __init__(self, name, short_code, multiple, tradeManager):
        # NOTE: All the below properties should be set by the Derived Class (Specific to each strategy)

        self.name = name  # strategy name
        self.short_code = short_code
        self.tradeManager = tradeManager  # Keep reference to trade manager
        self.enabled = True  # Strategy will be run only when it is enabled
        self.productType = ProductType.MIS  # MIS/NRML/CNC etc
        self.symbols = []  # List of stocks to be traded under this strategy
        self.slPercentage = 0
        self.targetPercentage = 0
        # When to start the strategy. Default is Market start time
        self.startTimestamp = Utils.getMarketStartTime(
            tradeManager.symbolToCMPMap["exchange_timestamp"]
        )
        self.stopTimestamp = None  # This is not square off timestamp. This is the timestamp after which no new trades will be placed under this strategy but existing trades continue to be active.
        self.squareOffTimestamp = None  # Square off time
        self.maxTradesPerDay = 1  # Max number of trades per day under this strategy
        self.isFnO = True  # Does this strategy trade in FnO or not
        self.strategySL = 0
        self.strategyTarget = 0
        self.highestPnl = 0
        self.lowestPnl = 0

        # Load all trades of this strategy into self.trades on restart of app
        self.trades = self.tradeManager.getAllTradesByStrategy(self.getName())
        self.expiryDay = 1
        self.symbol = "BANKNIFTY"
        self.daysToExpiry = Utils.findNumberOfDaysBeforeWeeklyExpiryDay(
            self.symbol,
            self.expiryDay,
            dateTimeObj=tradeManager.symbolToCMPMap.get("exchange_timestamp"),
        )
        self.multiple = multiple
        self.exchange = "NFO"
        self.equityExchange = "NSE"

        if tradeManager is not None and hasattr(tradeManager, "strategiesData"):
            strategyData = tradeManager.strategiesData.get(name, None)
            if strategyData is not None and isinstance(strategyData, dict):
                self.strategyData = strategyData

        # Register strategy with trade manager
        self.tradeManager.registerStrategy(self)

    def getExpiryDate(self, datetimeObj=None):
        if datetimeObj is None:
            datetimeObj = self.tradeManager.symbolToCMPMap["exchange_timestamp"]
        if getattr(self, "expiryType", "weekly") == "monthly":
            return Utils.getMonthlyExpiryDayDate(datetimeObj=datetimeObj, expiryDay=self.expiryDay)
        return Utils.getWeeklyExpiryDayDate(
            self.symbol, dateTimeObj=datetimeObj, expiryDay=self.expiryDay
        )

    def getName(self):
        return self.name

    def isEnabled(self):
        return self.enabled

    def setDisabled(self):
        self.enabled = False

    def getMultiple(self):
        return float(self.multiple)

    def getLotSize(self, tradingSymbol=None):
        """Return the lot size for *tradingSymbol* (or any strategy option).
        Falls back to SYMBOL_CONFIG when the instrument is not in the broker map
        (e.g. past-expiry options during backtest)."""
        if tradingSymbol:
            isd = Instruments.getInstrumentDataBySymbol(self.short_code, tradingSymbol)
            if isd.get('lot_size'):
                return isd['lot_size']
        return BaseStrategy.SYMBOL_CONFIG[self.symbol]['lot_size']

    def getLots(self):
        lots = (
            self.tradeManager.algoConfig.getLots(
                self.getName(),
                self.symbol,
                self.expiryDay,
                expiryType=getattr(self, "expiryType", "weekly"),
            )
            * self.getMultiple()
        )

        return ceil(lots)

    def process(self):
        # Implementation is specific to each strategy - To defined in derived class
        logging.info("BaseStrategy process is called.")
        pass

    def isTargetORSLHit(self):
        totalPnl = sum([trade.pnl for trade in self.trades])
        if totalPnl > self.highestPnl:
            self.highestPnl = totalPnl
        if totalPnl < self.lowestPnl:
            self.lowestPnl = totalPnl

        if self.strategySL == 0 and self.strategyTarget == 0:
            return None
        exitTrade = False
        reason = None

        if totalPnl < (self.strategySL * self.getLots()):
            if self.strategySL < 0:
                exitTrade = True
                reason = TradeExitReason.STRATEGY_SL_HIT
            if self.strategySL > 0:
                exitTrade = True
                reason = TradeExitReason.STRATEGY_TRAIL_SL_HIT
        elif self.strategyTarget > 0 and totalPnl > (self.strategyTarget * self.getLots()):
            self.strategySL = 0.9 * totalPnl / self.getLots()
            logging.warning(
                "Strategy Target %d hit for %s @ PNL per lot = %d, Updated SL to %d ",
                self.strategyTarget,
                self.getName(),
                totalPnl / self.getLots(),
                self.strategySL,
            )
            self.strategyTarget = 0  # no more targets, will trail SL
        elif self.strategySL > 0 and self.strategySL * 1.2 < totalPnl / self.getLots():
            self.strategySL = 0.9 * totalPnl / self.getLots()
            logging.warning(
                "Updated Strategy SL for %s to %d @ PNL per lot = %d",
                self.getName(),
                self.strategySL,
                totalPnl / self.getLots(),
            )

        if exitTrade:
            logging.warning(
                "Strategy SL Hit for %s at %d with PNL per lot = %d",
                self.getName(),
                self.strategySL,
                totalPnl / self.getLots(),
            )
            return reason
        else:
            return None

    def canTradeToday(self):
        # if the run is not set, it will default to -1, thus wait
        while self.getLots() == -1:
            if not self.tradeManager.is_alive():
                return False
            time.sleep(2)

        # strategy will run only if the number of lots is > 0
        return self.getLots() > 0

    def getVIXThreshold(self):
        return 0

    def run(self):

        self.fromDict(self.strategyData)

        if self.strategyData is None:  # Enabled status, SLs and target may have been adjusted

            # NOTE: This should not be overriden in Derived class
            if self.enabled == False:
                self.tradeManager.deRgisterStrategy(self)
                logging.warning("%s: Not going to run strategy as its not enabled.", self.getName())
                return

            if self.strategySL > 0:
                self.tradeManager.deRgisterStrategy(self)
                logging.warning("Strategy SL should be a -ve number")
                return

            self.strategySL = self.strategySL * Utils.getVIXAdjustment(self.short_code)
            self.strategyTarget = self.strategyTarget * Utils.getVIXAdjustment(self.short_code)

            if Utils.isMarketClosedForTheDay(Utils.getExchangeTimestamp(self.short_code, tradeManager=self.tradeManager)):
                self.tradeManager.deRgisterStrategy(self)
                logging.warning(
                    "%s: Not going to run strategy as market is closed.", self.getName()
                )
                return

        for trade in self.trades:
            if trade.exitReason not in [
                None,
                TradeExitReason.SL_HIT,
                TradeExitReason.TARGET_HIT,
                TradeExitReason.TRAIL_SL_HIT,
                TradeExitReason.MANUAL_EXIT,
            ]:
                logging.warning(
                    "Exiting %s as a trade found with %s", self.getName(), trade.exitReason
                )
                return  # likely something at strategy level or broker level, won't continue

        if self.canTradeToday() == False:
            self.tradeManager.deRgisterStrategy(self)
            logging.warning(
                "%s: Not going to run strategy as it cannot be traded today.", self.getName()
            )
            return

        now = Utils.getExchangeTimestamp(self.short_code, tradeManager=self.tradeManager)
        if now < Utils.getMarketStartTime():
            Utils.waitTillMarketOpens(self.getName())

        if now < self.startTimestamp:
            waitSeconds = Utils.getEpoch(self.startTimestamp) - Utils.getEpoch(now)
            logging.info(
                "%s: Waiting for %d seconds till startegy start timestamp reaches...",
                self.getName(),
                waitSeconds,
            )
            while waitSeconds > 0:
                if not self.tradeManager.is_alive():
                    logging.warning(
                        "%s: TradeManager died while waiting to start. Exiting.", self.getName()
                    )
                    return
                waitSeconds = Utils.getEpoch(self.startTimestamp) - Utils.getEpoch(
                    Utils.getExchangeTimestamp(self.short_code, tradeManager=self.tradeManager)
                )
                time.sleep(1)

        if self.getVIXThreshold() > self.tradeManager.symbolToCMPMap["INDIA VIX"]:
            self.tradeManager.deRgisterStrategy(self)
            logging.warning(
                "%s: Not going to conitnue strategy as VIX threshold is not met today.",
                self.getName(),
            )
            return

        # Run in an loop and keep processing
        while True:

            if (
                Utils.isMarketClosedForTheDay(Utils.getExchangeTimestamp(self.short_code, tradeManager=self.tradeManager))
                or not self.isEnabled()
            ):
                logging.warning(
                    "%s: Exiting the strategy as market closed or strategy was disabled.",
                    self.getName(),
                )
                break

            if not self.tradeManager.is_alive():
                logging.warning(
                    "%s: TradeManager is no longer alive. Exiting strategy loop.", self.getName()
                )
                break

            now = Utils.getExchangeTimestamp(self.short_code, tradeManager=self.tradeManager)
            if now > self.squareOffTimestamp:
                self.setDisabled()
                logging.warning(
                    "%s: Disabled the strategy as Squareoff time is passed.", self.getName()
                )

                return

            try:
                self.process()
            except Exception as e:
                logging.error(
                    "%s: Exception in process(), squaring off all trades and disabling strategy: %s",
                    self.getName(),
                    str(e),
                    exc_info=True,
                )
                self.tradeManager.squareOffStrategy(self, TradeExitReason.STRATEGY_ERROR)
                return

            waitSeconds = 5 - (now.second % 5) + 3
            time.sleep(waitSeconds)

    def shouldPlaceTrade(self, trade, tick):
        # Each strategy should call this function from its own shouldPlaceTrade() method before working on its own logic
        if trade == None:
            return False
        if not self.isEnabled():
            self.tradeManager.disableTrade(trade, "StrategyDisabled")
            return False
        if trade.qty == 0:
            self.tradeManager.disableTrade(trade, "InvalidQuantity")
            return False

        now = Utils.getExchangeTimestamp(self.short_code, tradeManager=self.tradeManager)
        if now > self.stopTimestamp:
            self.tradeManager.disableTrade(trade, "NoNewTradesCutOffTimeReached")
            return False

        numOfTradesPlaced = self.tradeManager.getNumberOfTradesPlacedByStrategy(self.getName())
        if numOfTradesPlaced >= self.maxTradesPerDay:
            self.tradeManager.disableTrade(trade, "MaxTradesPerDayReached")
            return False

        return True

    def addTradeToList(self, trade):
        if trade != None:
            self.trades.append(trade)

    def getTrailingSL(self, trade):
        return 0

    def generateTrade(
        self,
        optionSymbol,
        direction,
        numLots,
        lastTradedPrice,
        slPercentage=0,
        slPrice=0,
        targetPrice=0,
        placeMarketOrder=True,
    ):
        if lastTradedPrice == 0:
            logging.warning("%s: lastTradedPrice is 0 for %s", self.getName(), optionSymbol)
            raise ZeroPriceError(f"Zero price for {optionSymbol}")
        trade = Trade(optionSymbol, self.getName())
        trade.isOptions = True
        trade.exchange = self.exchange
        trade.direction = direction
        trade.productType = self.productType
        trade.placeMarketOrder = placeMarketOrder
        trade.requestedEntry = lastTradedPrice
        trade.timestamp = Utils.getEpoch(self.startTimestamp)  # setting this to strategy timestamp

        trade.underLying = self.symbol
        trade.stopLossPercentage = slPercentage
        trade.stopLoss = (
            slPrice  # if set to 0, then set stop loss will be set after entry via trailingSL method
        )
        trade.target = targetPrice

        trade.qty = self.getLotSize(optionSymbol) * numLots

        trade.intradaySquareOffTimestamp = Utils.getEpoch(self.squareOffTimestamp)
        # Hand over the trade to TradeManager
        self.tradeManager.addNewTrade(trade)

    def generateTradeWithSLPrice(
        self,
        optionSymbol,
        direction,
        numLots,
        lastTradedPrice,
        underLying,
        underLyingStopLossPercentage,
        placeMarketOrder=True,
    ):
        trade = Trade(optionSymbol, self.getName())
        trade.isOptions = True
        trade.exchange = self.exchange
        trade.direction = direction
        trade.productType = self.productType
        trade.placeMarketOrder = placeMarketOrder
        trade.requestedEntry = lastTradedPrice
        trade.timestamp = Utils.getEpoch(self.startTimestamp)  # setting this to strategy timestamp

        trade.underLying = underLying
        trade.stopLossUnderlyingPercentage = underLyingStopLossPercentage

        trade.qty = self.getLotSize(optionSymbol) * numLots

        trade.stopLoss = 0
        trade.target = 0  # setting to 0 as no target is applicable for this trade

        trade.intradaySquareOffTimestamp = Utils.getEpoch(self.squareOffTimestamp)
        # Hand over the trade to TradeManager
        self.tradeManager.addNewTrade(trade)

    def _getFuturePrice(self, roundToNearestStrike=100):
        """Return the future price to use as the ATM anchor for strike selection.

        On monthly-expiry weeks the actual futures contract is the source of truth,
        so we use it directly.  On weekly-expiry weeks we compute a synthetic future
        (ATM_strike + CE_premium - PE_premium).  The spot index is preferred as the
        ATM anchor for the synthetic; if unavailable we fall back to the futures quote.
        """
        futureSymbol = Utils.prepareMonthlyExpiryFuturesSymbol(
            self.symbol, self.expiryDay, datetimeObj=Utils.getExchangeTimestamp(self.short_code, tradeManager=self.tradeManager)
        )
        isMonthlyExpiryWeek = Utils.isTodayMonthlyExpiryDay(self.symbol, self.expiryDay)

        if isMonthlyExpiryWeek:
            quote = self.getQuote(futureSymbol)
            if quote is not None and quote.lastTradedPrice != 0:
                return quote.lastTradedPrice
            # Futures data unavailable (e.g. not in QuestDB for backtest) — fall back to spot index
            indexSymbol = getattr(self, "indexSymbol", None)
            if indexSymbol:
                quote = self.getIndexQuote(indexSymbol)
                if quote is not None and quote.lastTradedPrice != 0:
                    logging.warning(
                        "%s: Futures %s unavailable, falling back to index %s as anchor",
                        self.getName(),
                        futureSymbol,
                        indexSymbol,
                    )
                    return quote.lastTradedPrice
            logging.error(
                "%s: Could not get quote for %s or index fallback", self.getName(), futureSymbol
            )
            return None

        # Weekly expiry — compute synthetic future price
        # Prefer index spot as anchor; fall back to futures
        anchorPrice = None
        indexSymbol = getattr(self, "indexSymbol", None)
        if indexSymbol:
            quote = self.getIndexQuote(indexSymbol, exchange=self.equityExchange)
            if quote is not None and quote.lastTradedPrice != 0:
                anchorPrice = quote.lastTradedPrice
        if anchorPrice is None:
            quote = self.getQuote(futureSymbol)
            if quote is not None and quote.lastTradedPrice != 0:
                anchorPrice = quote.lastTradedPrice
                logging.info(
                    "%s: Index unavailable, using futures %s as anchor for synthetic price",
                    self.getName(),
                    futureSymbol,
                )
        if anchorPrice is None:
            logging.error("%s: Could not get quote for %s", self.getName(), futureSymbol)
            return None

        atmStrike = Utils.getNearestStrikePrice(anchorPrice, roundToNearestStrike)
        atmCEPremium = self.getQuote(self.prepareOptionSymbol(atmStrike, "CE")).lastTradedPrice
        atmPEPremium = self.getQuote(self.prepareOptionSymbol(atmStrike, "PE")).lastTradedPrice
        syntheticPrice = atmStrike + atmCEPremium - atmPEPremium
        logging.info(
            "%s: Synthetic future price = %.2f (anchor=%.2f, ATM=%d, CE=%.2f, PE=%.2f)",
            self.getName(),
            syntheticPrice,
            anchorPrice,
            atmStrike,
            atmCEPremium,
            atmPEPremium,
        )
        return syntheticPrice

    def getATMStrike(self, strikesAway=0):
        """Return ATM (or N strikes away) CE/PE symbols using spot index price as anchor.

        Args:
            strikesAway: Offset from ATM in number of strikes (0 = ATM, 1 = 1 OTM, -1 = 1 ITM)

        Returns:
            (ceSymbol, peSymbol) tuple, or (None, None) on error
        """
        quote = self.getIndexQuote(self.indexSymbol, exchange=self.equityExchange)
        if quote is None or quote.lastTradedPrice == 0:
            logging.error("%s: Could not get spot quote for %s", self.getName(), self.indexSymbol)
            return None, None
        strike = Utils.getNearestStrikePrice(quote.lastTradedPrice, self.symbolStrikeInterval)
        strike += strikesAway * self.symbolStrikeInterval
        return self.prepareOptionSymbol(strike, "CE"), self.prepareOptionSymbol(strike, "PE")

    def getStrikeWithNearestPremium(
        self, optionType, nearestPremium, roundToNearestStrike=100, underlyingPrice=None
    ):
        # Get the nearest premium strike price
        if underlyingPrice is None:
            underlyingPrice = self._getFuturePrice(roundToNearestStrike)
            if underlyingPrice is None:
                return

        strikePrice = Utils.getNearestStrikePrice(underlyingPrice, roundToNearestStrike)
        itmSign = -1 if optionType == "CE" else 1
        otmSign = -itmSign

        # --- Phase 1: coarse ITM jump (5 strikes at a time) until premium >= nearestPremium ---
        maxStrikeDistance = 0.20 * underlyingPrice
        premium = self.getQuote(self.prepareOptionSymbol(strikePrice, optionType)).lastTradedPrice
        phase1Jumped = False
        while premium < nearestPremium:
            strikePrice += itmSign * 5 * roundToNearestStrike
            if abs(strikePrice - underlyingPrice) > maxStrikeDistance:
                raise ValueError(
                    "%s: getStrikeWithNearestPremium strike %s is more than 20%% away from underlying %s"
                    % (self.getName(), strikePrice, underlyingPrice)
                )
            premium = self.getQuote(
                self.prepareOptionSymbol(strikePrice, optionType)
            ).lastTradedPrice
            phase1Jumped = True

        # Step back 5 strikes so we approach the target from the OTM side one strike at a time.
        # Only do this if Phase 1 actually jumped — if ATM premium was already above target,
        # stepping back would overshoot deep OTM.
        if phase1Jumped:
            strikePrice += otmSign * 5 * roundToNearestStrike
            premium = self.getQuote(
                self.prepareOptionSymbol(strikePrice, optionType)
            ).lastTradedPrice

        lastPremium = premium
        lastStrike = strikePrice

        # --- Phase 2: fine OTM walk one strike at a time until premium drops below target ---
        while True:
            try:
                if abs(strikePrice - underlyingPrice) > maxStrikeDistance:
                    raise ValueError(
                        "%s: getStrikeWithNearestPremium strike %s is more than 20%% away from underlying %s"
                        % (self.getName(), strikePrice, underlyingPrice)
                    )
                symbol = self.prepareOptionSymbol(strikePrice, optionType)
                try:
                    Instruments.getInstrumentDataBySymbol(self.short_code, symbol)
                except KeyError:
                    logging.info("%s: Could not get instrument for %s", self.getName(), symbol)
                    return lastStrike, lastPremium

                quote = self.getQuote(symbol)

                if quote.totalSellQuantity == 0 and quote.totalBuyQuantity == 0:
                    quote = self.getQuote(symbol)  # lets try one more time.

                premium = quote.lastTradedPrice

                if premium > nearestPremium:
                    lastPremium = premium
                    lastStrike = strikePrice
                    strikePrice += otmSign * roundToNearestStrike
                else:
                    # quote.lastTradedPrice < quote.upperCircuitLimit and quote.lastTradedPrice > quote.lowerCiruitLimit and \
                    if (
                        (lastPremium - nearestPremium) > (nearestPremium - premium)
                        and quote.volume > 0
                        and quote.totalSellQuantity > 0
                        and quote.totalBuyQuantity > 0
                    ):
                        return strikePrice, premium
                    else:
                        logging.info(
                            "%s: Returning previous strike for %s as vol = %s sell = %s buy = %s",
                            self.getName(),
                            symbol,
                            quote.volume,
                            quote.totalSellQuantity,
                            quote.totalBuyQuantity,
                        )
                        return lastStrike, lastPremium
            except KeyError:
                return lastStrike, lastPremium

    def getStrikeWithMinimumPremium(self, optionType, minimumPremium, roundToNearestStrike=100):
        # Get the nearest premium strike price
        underlyingPrice = self._getFuturePrice(roundToNearestStrike)
        if underlyingPrice is None:
            return

        strikePrice = Utils.getNearestStrikePrice(underlyingPrice, roundToNearestStrike)
        itmSign = -1 if optionType == "CE" else 1
        otmSign = -itmSign

        # --- Phase 1: coarse ITM jump (5 strikes at a time) until premium >= minimumPremium ---
        maxStrikeDistance = 0.20 * underlyingPrice
        premium = self.getQuote(
            Utils.prepareWeeklyOptionsSymbol(
                self.symbol, strikePrice, optionType, expiryDay=self.expiryDay
            )
        ).lastTradedPrice
        phase1Jumped = False
        while premium < minimumPremium:
            strikePrice += itmSign * 5 * roundToNearestStrike
            if abs(strikePrice - underlyingPrice) > maxStrikeDistance:
                raise ValueError(
                    "%s: getStrikeWithMinimumPremium strike %s is more than 20%% away from underlying %s"
                    % (self.getName(), strikePrice, underlyingPrice)
                )
            premium = self.getQuote(
                Utils.prepareWeeklyOptionsSymbol(
                    self.symbol, strikePrice, optionType, expiryDay=self.expiryDay
                )
            ).lastTradedPrice
            phase1Jumped = True

        # Only step back if Phase 1 actually jumped
        if phase1Jumped:
            strikePrice += otmSign * 5 * roundToNearestStrike

        lastPremium = -1
        lastStrike = strikePrice

        while True:
            try:
                if abs(strikePrice - underlyingPrice) > maxStrikeDistance:
                    raise ValueError(
                        "%s: getStrikeWithMinimumPremium strike %s is more than 20%% away from underlying %s"
                        % (self.getName(), strikePrice, underlyingPrice)
                    )
                symbol = Utils.prepareWeeklyOptionsSymbol(
                    self.symbol, strikePrice, optionType, expiryDay=self.expiryDay
                )
                Instruments.getInstrumentDataBySymbol(self.short_code, symbol)
                quote = self.getQuote(symbol)

                if quote.totalSellQuantity == 0 and quote.totalBuyQuantity == 0:
                    quote = self.getQuote(symbol)  # lets try one more time.

                premium = quote.lastTradedPrice

                if premium < minimumPremium:
                    return lastStrike, lastPremium

                lastStrike = strikePrice
                lastPremium = premium

                if optionType == "CE":
                    strikePrice = strikePrice + roundToNearestStrike
                else:
                    strikePrice = strikePrice - roundToNearestStrike
            except KeyError:
                return lastStrike, lastPremium

    def getStrikeWithMaximumPremium(self, optionType, maximumPremium, roundToNearestStrike=100):
        # Get the nearest premium strike price
        underlyingPrice = self._getFuturePrice(roundToNearestStrike)
        if underlyingPrice is None:
            return

        strikePrice = Utils.getNearestStrikePrice(underlyingPrice, roundToNearestStrike)
        itmSign = -1 if optionType == "CE" else 1
        otmSign = -itmSign

        # --- Phase 1: coarse ITM jump (5 strikes at a time) until premium >= maximumPremium ---
        maxStrikeDistance = 0.20 * underlyingPrice
        premium = self.getQuote(
            Utils.prepareWeeklyOptionsSymbol(
                self.symbol, strikePrice, optionType, expiryDay=self.expiryDay
            )
        ).lastTradedPrice
        phase1Jumped = False
        while premium < maximumPremium:
            strikePrice += itmSign * 5 * roundToNearestStrike
            if abs(strikePrice - underlyingPrice) > maxStrikeDistance:
                raise ValueError(
                    "%s: getStrikeWithMaximumPremium strike %s is more than 20%% away from underlying %s"
                    % (self.getName(), strikePrice, underlyingPrice)
                )
            premium = self.getQuote(
                Utils.prepareWeeklyOptionsSymbol(
                    self.symbol, strikePrice, optionType, expiryDay=self.expiryDay
                )
            ).lastTradedPrice
            phase1Jumped = True

        # Only step back if Phase 1 actually jumped
        if phase1Jumped:
            strikePrice += otmSign * 5 * roundToNearestStrike

        lastPremium = -1
        lastStrike = strikePrice

        while True:
            try:
                if abs(strikePrice - underlyingPrice) > maxStrikeDistance:
                    raise ValueError(
                        "%s: getStrikeWithMaximumPremium strike %s is more than 20%% away from underlying %s"
                        % (self.getName(), strikePrice, underlyingPrice)
                    )
                symbol = Utils.prepareWeeklyOptionsSymbol(
                    self.symbol, strikePrice, optionType, expiryDay=self.expiryDay
                )
                Instruments.getInstrumentDataBySymbol(self.short_code, symbol)
                quote = self.getQuote(symbol)

                if quote.totalSellQuantity == 0 and quote.totalBuyQuantity == 0:
                    quote = self.getQuote(symbol)  # lets try one more time.

                premium = quote.lastTradedPrice

                if premium < maximumPremium:
                    return strikePrice, premium

                lastStrike = strikePrice
                lastPremium = premium

                if optionType == "CE":
                    strikePrice = strikePrice + roundToNearestStrike
                else:
                    strikePrice = strikePrice - roundToNearestStrike
            except KeyError:
                return lastStrike, lastPremium

    def asDict(self):
        dict = {}
        dict["name"] = self.name
        dict["enabled"] = self.enabled
        dict["strategySL"] = self.strategySL
        dict["strategyTarget"] = self.strategyTarget
        dict["highestPnl"] = self.highestPnl
        dict["lowestPnl"] = self.lowestPnl
        return dict

    def fromDict(self, dict):
        if not dict is None:
            self.enabled = dict["enabled"]
            self.strategySL = dict["strategySL"]
            self.strategyTarget = dict["strategyTarget"]
            self.highestPnl = dict.get("highestPnl", 0)
            self.lowestPnl = dict.get("lowestPnl", 0)

    def prepareOptionSymbol(self, strike, optionType):
        if getattr(self, "expiryType", "weekly") == "monthly":
            return Utils.prepareMonthlyOptionsSymbol(
                self.symbol, strike, optionType, expiryDay=self.expiryDay
            )
        return Utils.prepareWeeklyOptionsSymbol(
            self.symbol, strike, optionType, expiryDay=self.expiryDay
        )

    def getQuote(self, tradingSymbol, expiry=None):
        """
        Get quote for a trading symbol.
        In backtest mode, fetches from BacktestManager. In live mode, fetches from Quotes.

        Args:
          tradingSymbol: Symbol to get quote for
          expiry: Optional expiry date string (YYYY-MM-DD). If not provided, will be calculated from expiryDay.
        """

        if hasattr(self.tradeManager, "is_backtest_mode") and self.tradeManager.is_backtest_mode:
            # Pass underlying symbol for options/futures in backtest mode
            underlying = self.symbol if self.isFnO else None
            return self.tradeManager._getBacktestQuote(
                tradingSymbol, self.isFnO, self.exchange, underlying, self.expiryDay, expiry
            )
        else:
            return Quotes.getQuote(tradingSymbol, self.short_code, self.isFnO, self.exchange)

    def getIndexQuote(self, tradingSymbol, exchange="NSE"):
        """
        Get index quote for a trading symbol.
        In backtest mode, fetches from BacktestManager. In live mode, fetches from Quotes.
        """
        if hasattr(self.tradeManager, "is_backtest_mode") and self.tradeManager.is_backtest_mode:
            # Indices don't need underlying or expiry parameters
            return self.tradeManager._getBacktestQuote(
                tradingSymbol, False, exchange, None, expiryDay=None, expiry=None
            )
        else:
            return Quotes.getIndexQuote(tradingSymbol, self.short_code, exchange)
