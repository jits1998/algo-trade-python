import logging
from datetime import datetime

from core.Quotes import Quotes
from models.Direction import Direction
from models.ProductType import ProductType
from core.BaseStrategy import BaseStrategy
from trademgmt.TradeExitReason import TradeExitReason
from utils.Utils import Utils
from trademgmt.TradeState import TradeState

# Each strategy has to be derived from BaseStrategy
class NiftyExpiry1415(BaseStrategy):
  __instance = {}

  @staticmethod
  def getInstance(short_code): # singleton class
    if NiftyExpiry1415.__instance.get(short_code, None) == None:
      NiftyExpiry1415()
    return NiftyExpiry1415.__instance[short_code]

  def __init__(self, short_code, multiple, tradeManager):
    if NiftyExpiry1415.__instance.get(short_code, None) != None:
      #   raise Exception("This class is a singleton!")
      # else:
      NiftyExpiry1415.__instance[short_code] = self
    # Call Base class constructor
    super().__init__("NiftyExpiry1415", short_code, multiple, tradeManager)
    # Initialize all the properties specific to this strategy
    self.productType = ProductType.MIS
    self.symbols = []
    self.slPercentage = 0
    self.targetPercentage = 0
    self.startTimestamp = Utils.getTimeOfToDay(14, 15, 0, dateTimeObj=tradeManager.symbolToCMPMap['exchange_timestamp']) # When to start the strategy. Default is Market start time
    self.stopTimestamp = Utils.getTimeOfToDay(15, 19, 0, dateTimeObj=tradeManager.symbolToCMPMap['exchange_timestamp']) # This is not square off timestamp. This is the timestamp after which no new trades will be placed under this strategy but existing trades continue to be active.
    self.squareOffTimestamp = Utils.getTimeOfToDay(15, 19, 0, dateTimeObj=tradeManager.symbolToCMPMap['exchange_timestamp']) # Square off time
    self.maxTradesPerDay = 2 # (1 CE + 1 PE + Hedges) Max number of trades per day under this strategy
    self.capitalPerSet = 125000 # With hedge and SL order in system
    self.strategySL = 0
    self.strategyTarget = 0
    self.symbol = "NIFTY"
    self.indexSymbol = "NIFTY 50"
    self.daysToExpiry = Utils.findNumberOfDaysBeforeWeeklyExpiryDay(
        self.symbol, self.expiryDay,
        dateTimeObj=tradeManager.symbolToCMPMap.get('exchange_timestamp'))

  def process(self):
    now = Utils.getExchangeTimestamp(self.short_code, tradeManager=self.tradeManager)
    if now < self.startTimestamp or not self.isEnabled():
      return

    if len(self.trades) >= self.maxTradesPerDay or not self.isEnabled():
      if self.strategySL == 0:
        self.strategySL = self.getStrategySL()
      return
    
    if self.isTargetORSLHit():
      #self.setDisabled()
      return
    
    ceStrike, cePremium = self.getStrikeWithNearestPremium("CE", 50, roundToNearestStrike=50)
    peStrike, pePremium = self.getStrikeWithNearestPremium("PE", 50, roundToNearestStrike=50)

    CESymbol = Utils.prepareWeeklyOptionsSymbol(self.symbol, ceStrike, 'CE', expiryDay=self.expiryDay)
    PESymbol = Utils.prepareWeeklyOptionsSymbol(self.symbol, peStrike, 'PE', expiryDay=self.expiryDay)

    self.generateTrade(CESymbol, Direction.SHORT, self.getLots(), cePremium)
    self.generateTrade(PESymbol, Direction.SHORT, self.getLots(), pePremium)

  def getTrailingSL(self, trade):
    
    slPercentage = 75
    return Utils.roundToNSEPrice(trade.entry + trade.entry * slPercentage / 100)  

  def getStrategySL(self):
    if self.strategySL == 0:
      for trade in self.trades:
        if trade.tradeState in (TradeState.ACTIVE, TradeState.CREATED) and trade.entry == 0:
          self.strategySL = 0 # wait for all the orders to be placed.
          break
        else:
          if trade.direction == Direction.SHORT:
            self.strategySL -= 0.2 * trade.entry * trade.qty / self.getLots()
          else:
            self.strategySL += 0.2 * trade.entry * trade.qty / self.getLots()
      
    return self.strategySL

