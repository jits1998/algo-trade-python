from core.BaseStrategy import BaseStrategy
from models.ProductType import ProductType
from utils.Utils import Utils
from datetime import datetime

class ManualStrategy(BaseStrategy):

  skip_in_backtest = True
  skip_in_shadow = True

  __instance = {}

  @staticmethod
  def getInstance(short_code):  # singleton class
      if ManualStrategy.__instance.get(short_code, None) == None:
          ManualStrategy(short_code)
      return ManualStrategy.__instance[short_code]
  
  def __init__(self, short_code, multiple, tradeManager):

    if ManualStrategy.__instance.get(short_code, None) != None:
      raise Exception("This class is a singleton!")
    else:
      ManualStrategy.__instance[short_code] = self

    super().__init__("ManualStrategy", short_code, multiple, tradeManager)

    # When to start the strategy. Default is Market start time
    self.startTimestamp = Utils.getTimeOfToDay(9, 16, 0, dateTimeObj=tradeManager.symbolToCMPMap['exchange_timestamp'])
    self.productType = ProductType.MIS
    # This is not square off timestamp. This is the timestamp after which no new trades will be placed under this strategy but existing trades continue to be active.
    self.stopTimestamp = Utils.getTimeOfToDay(15, 24, 0, dateTimeObj=tradeManager.symbolToCMPMap['exchange_timestamp'])
    self.squareOffTimestamp = Utils.getTimeOfToDay(15, 24, 0, dateTimeObj=tradeManager.symbolToCMPMap['exchange_timestamp'])  # Square off time
    self.maxTradesPerDay = 10

  def process(self):
    now = Utils.getExchangeTimestamp(self.short_code, tradeManager=self.tradeManager)
    if now < self.startTimestamp or not self.isEnabled():
        return