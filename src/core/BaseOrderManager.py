

class BaseOrderManager:
  def __init__(self, broker, brokerHandle):
    self.broker = broker
    self.brokerHandle = brokerHandle

  def placeOrder(self, orderInputParams):
    pass

  def modifyOrder(self, order, orderModifyParams):
    pass

  def modifyOrderToMarket(self, order):
    pass

  def cancelOrder(self, order):
    pass

  def updateOrder(self, order, data):
    pass

  def fetchAndUpdateAllOrderDetails(self, orders):
    pass

  def getMaxOrderQuantity(self, tradingSymbol):
    return None  # No limit by default

  def convertToBrokerProductType(self, productType):
    return productType

  def convertToBrokerOrderType(self, orderType):
    return orderType

  def convertToBrokerDirection(self, direction):
    return direction
