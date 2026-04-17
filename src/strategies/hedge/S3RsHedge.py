import logging
from datetime import datetime

from core.BaseStrategy import BaseStrategy
from models.Direction import Direction
from models.ProductType import ProductType
from utils.Utils import Utils


class S3RsHedge (BaseStrategy):
    skip_in_shadow = True
    __instance = {}

    @staticmethod
    def getInstance(short_code):  # singleton class
        if S3RsHedge.__instance.get(short_code, None) == None:
            S3RsHedge(short_code)
        return S3RsHedge.__instance[short_code]

    def __init__(self, short_code, multiple, tradeManager):
        if S3RsHedge.__instance.get(short_code, None) != None and not tradeManager.is_backtest_mode:
            raise Exception("This class is a singleton!")
        else:
            S3RsHedge.__instance[short_code] = self
        # Call Base class constructor
        super().__init__("S3RsHedge", short_code, multiple, tradeManager)
        # Initialize all the properties specific to this strategy
        
        # When to start the strategy. Default is Market start time
        self.startTimestamp = Utils.getTimeOfToDay(9, 20, 0, dateTimeObj=tradeManager.symbolToCMPMap['exchange_timestamp'])
        self.productType = ProductType.MIS
        # This is not square off timestamp. This is the timestamp after which no new trades will be placed under this strategy but existing trades continue to be active.
        self.stopTimestamp = Utils.getTimeOfToDay(15, 24, 0, dateTimeObj=tradeManager.symbolToCMPMap['exchange_timestamp'])
        self.squareOffTimestamp = Utils.getTimeOfToDay(15, 24, 0, dateTimeObj=tradeManager.symbolToCMPMap['exchange_timestamp'])  # Square off time
        # (1 CE + 1 PE) Max number of trades per day under this strategy
        self.maxTradesPerDay = 2
        self.symbol = "SENSEX"
        self.indexSymbol = "SENSEX"
        self.symbolStrikeInterval = 100
        self.expiryDay = 3
        self.exchange = "BFO"
        self.equityExchange = "BSE"
        self.daysToExpiry = Utils.findNumberOfDaysBeforeWeeklyExpiryDay(
            self.symbol, self.expiryDay,
            dateTimeObj=tradeManager.symbolToCMPMap.get('exchange_timestamp'))

    def addTradeToList(self, trade):
        if trade != None:
            self.trades.append(trade)

    def process(self):
        now = Utils.getExchangeTimestamp(self.short_code, tradeManager=self.tradeManager)
        if now < self.startTimestamp or not self.isEnabled():
            return
        
        if Utils.isTodayOneDayBeforeWeeklyExpiryDay(self.symbol, self.expiryDay):
            self.stopTimestamp = Utils.getTimeOfToDay(14, 14, 0) 
            self.squareOffTimestamp = Utils.getTimeOfToDay(14, 14, 0)
        
        if not len(self.trades) == 0:
            return

        numLots = self.getLots()
    
        ceStrike, cePremium = self.getStrikeWithNearestPremium("CE", 3)
        logging.info('%s:: Recieved CE Strike %s CE Premium %s' % (self.short_code, ceStrike, cePremium))
        if not ceStrike == 0:
            ceSymbol = Utils.prepareWeeklyOptionsSymbol(self.symbol, ceStrike, "CE", expiryDay=self.expiryDay)
            self.generateTrade(ceSymbol, Direction.LONG, numLots, cePremium+0.1)

        peStrike, pePremium = self.getStrikeWithNearestPremium("PE", 3)
        logging.info('%s:: Recieved PE Strike %s PE Premium %s' % (self.short_code, peStrike, pePremium))
        if not peStrike == 0:
            peSymbol = Utils.prepareWeeklyOptionsSymbol(self.symbol, peStrike, "PE", expiryDay=self.expiryDay)
            self.generateTrade(peSymbol, Direction.LONG, numLots, pePremium+0.1)  

        return