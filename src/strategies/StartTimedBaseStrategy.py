from core.BaseStrategy import BaseStrategy
from models.ProductType import ProductType
from utils.Utils import Utils


class StartTimedBaseStrategy(BaseStrategy):

  #DO NOT call the base constructor, as it will override the start time and register with trademanager with overridden timestamp
  def __init__(self, name, short_code, startTime, multiple, tradeManager):
    self.name = name # strategy name
    self.short_code = short_code

    self.tradeManager = tradeManager  # Keep reference to trade manager
    self.enabled = True # Strategy will be run only when it is enabled
    self.productType = ProductType.MIS # MIS/NRML/CNC etc
    self.symbols = [] # List of stocks to be traded under this strategy
    self.slPercentage = 0
    self.targetPercentage = 0
    self.startTimestamp = startTime # When to start the strategy. Default is Market start time
    self.stopTimestamp = None # This is not square off timestamp. This is the timestamp after which no new trades will be placed under this strategy but existing trades continue to be active.
    self.squareOffTimestamp = None # Square off time
    self.maxTradesPerDay = 1 # Max number of trades per day under this strategy
    self.isFnO = True # Does this strategy trade in FnO or not
    self.strategySL = 0
    self.strategyTarget = 0
    self.highestPnl = 0
    self.lowestPnl = 0

    # Load all trades of this strategy into self.trades on restart of app
    self.trades = self.tradeManager.getAllTradesByStrategy(self.getName())
    self.expiryDay = 1
    self.symbol = "BANKNIFTY"
    self.daysToExpiry = Utils.findNumberOfDaysBeforeWeeklyExpiryDay(
        self.symbol, self.expiryDay,
        dateTimeObj=tradeManager.symbolToCMPMap.get('exchange_timestamp'))
    self.multiple = multiple

    if tradeManager is not None and hasattr(tradeManager, 'strategiesData'):
      strategyData = tradeManager.strategiesData.get(self.getName(), None)
      if strategyData is not None and isinstance(strategyData, dict):
        self.strategyData = strategyData

    # Register strategy with trade manager
    self.tradeManager.registerStrategy(self)
    
  def getName(self):
    return super().getName() + "_" + str(self.startTimestamp.time())

      
