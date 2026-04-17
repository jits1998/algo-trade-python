import logging
from datetime import datetime

from core.BaseStrategy import BaseStrategy
from models.Direction import Direction
from models.ProductType import ProductType
from trademgmt.TradeExitReason import TradeExitReason
from trademgmt.TradeState import TradeState
from utils.Utils import Utils

CANDLE_INTERVAL = 3  # 3-minute candles
CMF_PERIOD = 20      # standard Chaikin Money Flow period


class ChaikinMFBuy(BaseStrategy):
    """
    ATM options buying strategy using Chaikin Money Flow on 3-min candles.

    Entry: CMF crosses from negative to positive (prev CMF < 0, current CMF >= 0) → buy that leg
    Exit:  CMF falls back below 0 → exit that leg

    CE and PE are monitored independently — both can be active simultaneously.
    All trades squared off at squareOffTimestamp.
    """

    __instance = {}

    @staticmethod
    def getInstance(short_code, symbol="NIFTY"):
        key = f"{short_code}_{symbol}"
        return ChaikinMFBuy.__instance.get(key, None)

    def __init__(
        self,
        short_code,
        multiple,
        tradeManager,
        symbol="NIFTY",
        startTimestamp=None,
        exitTimestamp=None,
        strikesAwayFromATM=0,
    ):
        key = f"{short_code}_{symbol}"
        ChaikinMFBuy.__instance[key] = self

        super().__init__(f"ChaikinMFBuy_{symbol}", short_code, multiple, tradeManager)

        cfg = BaseStrategy.getSymbolConfig(symbol)
        self.symbol = symbol
        self.indexSymbol = cfg["indexSymbol"]
        self.symbolStrikeInterval = cfg["symbolStrikeInterval"]
        self.expiryDay = cfg["expiryDay"]
        self.exchange = cfg["exchange"]
        self.equityExchange = cfg["equityExchange"]
        self.expiryType = cfg["expiryType"]
        self.strikesAwayFromATM = strikesAwayFromATM

        dt = tradeManager.symbolToCMPMap["exchange_timestamp"]
        if self.expiryType == "monthly":
            self.expiryDate = Utils.getMonthlyExpiryDayDate(
                datetimeObj=dt, expiryDay=self.expiryDay
            ).strftime("%Y-%m-%d")
        else:
            self.expiryDate = Utils.getWeeklyExpiryDayDate(
                symbol, dateTimeObj=dt, expiryDay=self.expiryDay
            ).strftime("%Y-%m-%d")

        if startTimestamp is None:
            startTimestamp = Utils.getTimeOfToDay(11, 0, 0, dateTimeObj=dt)
        else:
            startTimestamp = Utils.getTimeOfToDay(
                startTimestamp.hour, startTimestamp.minute, startTimestamp.second, dateTimeObj=dt
            )

        if exitTimestamp is None:
            exitTimestamp = Utils.getTimeOfToDay(15, 15, 0, dateTimeObj=dt)
        else:
            exitTimestamp = Utils.getTimeOfToDay(
                exitTimestamp.hour, exitTimestamp.minute, exitTimestamp.second, dateTimeObj=dt
            )

        self.productType = ProductType.MIS
        self.isFnO = True
        self.startTimestamp = startTimestamp
        self.stopTimestamp = Utils.getTimeOfToDay(14, 0, 0, dateTimeObj=dt)
        self.squareOffTimestamp = exitTimestamp
        self.slPercentage = 0
        self.targetPercentage = 0
        self.strategySL = 0
        self.strategyTarget = 0
        self.maxTradesPerDay = 20

        if self.expiryType == "monthly":
            self.daysToExpiry = Utils.findNumberOfDaysBeforeMonthlyExpiryDay(
                expiryDay=self.expiryDay, dateTimeObj=dt
            )
        else:
            self.daysToExpiry = Utils.findNumberOfDaysBeforeWeeklyExpiryDay(
                symbol, self.expiryDay, dateTimeObj=dt
            )

        self.ceTrades = []
        self.peTrades = []
        for trade in self.trades:
            if trade.tradingSymbol.endswith("CE"):
                self.ceTrades.append(trade)
            else:
                self.peTrades.append(trade)

        self.atmCESymbol = None
        self.atmPESymbol = None
        self.lastActedCandleTime = None

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _lastClosedCandleBoundary(self, now):
        """Return the start of the current (incomplete) 3-min candle, used as calculation_time for getCMF."""
        mins_since_open = (now.hour * 60 + now.minute) - (9 * 60 + 15)
        completed_bars = mins_since_open // CANDLE_INTERVAL
        if completed_bars < 1:
            return None
        boundary_mins = completed_bars * CANDLE_INTERVAL
        open_mins = 9 * 60 + 15 + boundary_mins
        return now.replace(hour=open_mins // 60, minute=open_mins % 60, second=0, microsecond=0)

    def _getCMF(self, symbol, last_closed):
        """Returns {'cmf': float, 'prev_cmf': float} or None if insufficient data."""
        return self.tradeManager.quotes.getCMF(
            tradingSymbol=symbol,
            short_code=self.short_code,
            calculation_time=last_closed,
            period=CMF_PERIOD,
            units_per_candle=CANDLE_INTERVAL,
            unit_type="minutes",
            isFnO=True,
            exchange=self.exchange,
            underlying=self.indexSymbol,
            expiry_date=self.expiryDate,
        )

    def _activeTradeFor(self, leg):
        """Return the single ACTIVE or CREATED trade for 'CE' or 'PE', or None."""
        trades = self.ceTrades if leg == "CE" else self.peTrades
        for t in trades:
            if t.tradeState in (TradeState.ACTIVE, TradeState.CREATED):
                return t
        return None

    def _exitTrade(self, trade):
        self.tradeManager.squareOffTrade(trade, reason=TradeExitReason.SQUARE_OFF)
        logging.info("%s: Exited %s trade %s", self.getName(), trade.tradingSymbol, trade.tradeID)

    def _checkLeg(self, leg, active, symbol, last_closed):
        """
        Check CMF exit and entry signals for one leg (CE or PE).
        Returns True if an action was taken.
        """
        result = self._getCMF(symbol, last_closed)
        if result is None:
            return False
        current_cmf = result["cmf"]
        prev_cmf = result["prev_cmf"]

        logging.debug(
            "%s: %s CMF=%.4f  prev=%.4f", self.getName(), leg, current_cmf, prev_cmf
        )

        # Exit: CMF fell below 0
        if active and current_cmf < 0:
            logging.info(
                "%s: %s exit — CMF=%.4f < 0", self.getName(), leg, current_cmf
            )
            self._exitTrade(active)
            return True

        # Entry: CMF crossed from -ve to +ve with minimum threshold
        if active is None and prev_cmf < 0 and current_cmf >= 0.05:
            if len(self.trades) < self.maxTradesPerDay:
                quote = self.getQuote(symbol)
                if quote and quote.lastTradedPrice > 0:
                    logging.info(
                        "%s: %s entry — CMF crossed +ve (%.4f → %.4f)  ltp=%.2f",
                        self.getName(), leg, prev_cmf, current_cmf, quote.lastTradedPrice,
                    )
                    self.generateTrade(symbol, Direction.LONG, self.getLots(), quote.lastTradedPrice)
                    return True

        return False

    # ------------------------------------------------------------------
    # BaseStrategy interface
    # ------------------------------------------------------------------

    def prepareOptionSymbol(self, strike, optionType):
        if self.expiryType == "monthly":
            return Utils.prepareMonthlyOptionsSymbol(
                self.symbol, strike, optionType, expiryDay=self.expiryDay
            )
        return Utils.prepareWeeklyOptionsSymbol(
            self.symbol, strike, optionType, expiryDay=self.expiryDay
        )

    def addTradeToList(self, trade):
        if trade is not None:
            self.trades.append(trade)
            if trade.tradingSymbol.endswith("CE"):
                self.ceTrades.append(trade)
            else:
                self.peTrades.append(trade)

    def process(self):
        now = Utils.getExchangeTimestamp(self.short_code)
        if now < self.startTimestamp or not self.isEnabled():
            return

        # Only act once per closed 3-min candle
        last_closed = self._lastClosedCandleBoundary(now)
        if last_closed is None or last_closed == self.lastActedCandleTime:
            return

        ce_active = self._activeTradeFor("CE")
        pe_active = self._activeTradeFor("PE")

        # Resolve ATM once at session start, never change mid-session
        if self.atmCESymbol is None:
            ceSymbol, peSymbol = self.getATMStrike(self.strikesAwayFromATM)
            if ceSymbol is None:
                return
            self.atmCESymbol = ceSymbol
            self.atmPESymbol = peSymbol
            logging.info("%s: ATM locked — CE=%s PE=%s", self.getName(), self.atmCESymbol, self.atmPESymbol)

        acted = False
        if self._checkLeg("CE", ce_active, self.atmCESymbol, last_closed):
            acted = True
        if self._checkLeg("PE", pe_active, self.atmPESymbol, last_closed):
            acted = True

        self.lastActedCandleTime = last_closed

    def getTrailingSL(self, trade):
        return 0

    def asDict(self):
        d = super().asDict()
        d["atmCESymbol"] = self.atmCESymbol
        d["atmPESymbol"] = self.atmPESymbol
        d["lastActedCandleTime"] = (
            self.lastActedCandleTime.strftime("%Y-%m-%d %H:%M:%S")
            if self.lastActedCandleTime
            else None
        )
        return d

    def fromDict(self, d):
        if d:
            self.atmCESymbol = d.get("atmCESymbol")
            self.atmPESymbol = d.get("atmPESymbol")
            raw = d.get("lastActedCandleTime")
            self.lastActedCandleTime = datetime.strptime(raw, "%Y-%m-%d %H:%M:%S") if raw else None
        super().fromDict(d)
