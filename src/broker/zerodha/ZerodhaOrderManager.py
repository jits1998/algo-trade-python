import logging

from core.BaseOrderManager import BaseOrderManager
from ordermgmt.Order import Order
from ordermgmt.OrderInputParams import OrderInputParams

from models.ProductType import ProductType
from models.OrderType import OrderType
from models.Direction import Direction
from models.OrderStatus import OrderStatus
from utils.Utils import Utils

class ZerodhaOrderManager(BaseOrderManager):

  def __init__(self, brokerHandle, short_code = None):
    super().__init__("zerodha", brokerHandle)
    self.short_code = short_code

  _FREEZE_LIMITS = {'BANKNIFTY': 600, 'SENSEX': 1000, 'NIFTY': 1800, 'BANKEX': 600}

  def getMaxOrderQuantity(self, tradingSymbol):
    for key, value in self._FREEZE_LIMITS.items():
      if tradingSymbol.startswith(key):
        return value
    return 0  # unknown symbol — caller should treat as error

  def placeOrder(self, orderInputParams):
    logging.debug('%s:%s:: Going to place order with params %s', self.broker, self.short_code, orderInputParams)
    kite = self.brokerHandle
    orderInputParams.qty = int(orderInputParams.qty)

    try:
      orderId = kite.place_order(
        variety=kite.VARIETY_REGULAR,
        exchange=orderInputParams.exchange if orderInputParams.isFnO == True else kite.EXCHANGE_NSE,
        tradingsymbol=orderInputParams.tradingSymbol,
        transaction_type=self.convertToBrokerDirection(orderInputParams.direction),
        quantity=orderInputParams.qty,
        price=orderInputParams.price,
        trigger_price=orderInputParams.triggerPrice,
        product=self.convertToBrokerProductType(orderInputParams.productType),
        order_type=self.convertToBrokerOrderType(orderInputParams.orderType),
        tag=orderInputParams.tag[:20],
        market_protection=kite.MARKET_PROTECTION_AUTO)

      logging.info('%s:%s:: Order placed successfully, orderId = %s with tag: %s', self.broker, self.short_code, orderId, orderInputParams.tag)
      order = Order(orderInputParams)
      order.orderId = orderId
      order.orderPlaceTimestamp = Utils.getEpoch(short_code=self.short_code)
      order.lastOrderUpdateTimestamp = Utils.getEpoch(short_code=self.short_code)
      return order
    except Exception as e:
      if "Too many requests" in str(e):
        logging.info('%s:%s retrying order placement in 1 s', self.broker, self.short_code)
        import time
        time.sleep(1)
        return self.placeOrder(orderInputParams)
      logging.info('%s:%s Order placement failed: %s', self.broker, self.short_code, str(e))
      if "Trigger price for stoploss" in str(e):
        orderInputParams.orderType = OrderType.LIMIT
        return self.placeOrder(orderInputParams)
      raise Exception(str(e))

  def modifyOrder(self, order, orderModifyParams, tradeQty):
    logging.info('%s:%s:: Going to modify order with params %s', self.broker, self.short_code, orderModifyParams)
    
    if order.orderType == OrderType.SL_LIMIT and orderModifyParams.newTriggerPrice == order.triggerPrice:
      logging.info('%s:%s:: Not Going to modify order with params %s', self.broker, self.short_code, orderModifyParams)
      #nothing to modify
      return order
    elif order.orderType == OrderType.LIMIT and orderModifyParams.newPrice < 0 or orderModifyParams.newPrice == order.price:
      #nothing to modify
      logging.info('%s:%s:: Not Going to modify order with params %s', self.broker, self.short_code, orderModifyParams)
      return order
    
    kite = self.brokerHandle
    freeze_limit = 900 if order.tradingSymbol.startswith("BANK") else 1800

    try:
      orderId = kite.modify_order(
        variety= kite.VARIETY_REGULAR if tradeQty<=freeze_limit else kite.VARIETY_ICEBERG,
        order_id=order.orderId,
        quantity=int(orderModifyParams.newQty) if orderModifyParams.newQty > 0 else None,
        price=orderModifyParams.newPrice if orderModifyParams.newPrice > 0 else None,
        trigger_price=orderModifyParams.newTriggerPrice if orderModifyParams.newTriggerPrice > 0 else None,
        order_type=orderModifyParams.newOrderType if orderModifyParams.newOrderType != None else None,
        market_protection=kite.MARKET_PROTECTION_AUTO)

      logging.info('%s:%s Order modified successfully for orderId = %s', self.broker, self.short_code, orderId)
      order.lastOrderUpdateTimestamp = Utils.getEpoch(short_code=self.short_code)
      return order
    except Exception as e:
      if "Too many requests" in str(e):
        logging.info('%s:%s retrying order modification in 1 s for %s', self.broker, self.short_code, order.orderId)
        import time
        time.sleep(1)
        self.modifyOrder(order, orderModifyParams, tradeQty)
      logging.info('%s:%s Order %s modify failed: %s', self.broker, self.short_code, order.orderId, str(e))
      raise Exception(str(e))

  def modifyOrderToMarket(self, order):
    raise Exception("Method not to be called")
    # logging.debug('%s:%s:: Going to modify order with params %s', self.broker, self.short_code)
    # kite = self.brokerHandle
    # try:
    #   orderId = kite.modify_order(
    #     variety= kite.VARIETY_REGULAR,
    #     order_id=order.orderId,
    #     order_type=kite.ORDER_TYPE_MARKET)

    #   logging.info('%s:%s Order modified successfully to MARKET for orderId = %s', self.broker, self.short_code, orderId)
    #   order.lastOrderUpdateTimestamp = Utils.getEpoch()
    #   return order
    # except Exception as e:
    #   logging.info('%s:%s Order modify to market failed: %s', self.broker, self.short_code, str(e))
    #   raise Exception(str(e))

  def cancelOrder(self, order):
    logging.debug('%s:%s Going to cancel order %s', self.broker, self.short_code, order.orderId)
    kite = self.brokerHandle
    freeze_limit = 900 if order.tradingSymbol.startswith("BANK") else 1800
    try:
      orderId = kite.cancel_order(
        variety= kite.VARIETY_REGULAR if order.qty<=freeze_limit else kite.VARIETY_ICEBERG,
        order_id=order.orderId)

      logging.info('%s:%s Order cancelled successfully, orderId = %s', self.broker, self.short_code, orderId)
      order.lastOrderUpdateTimestamp = Utils.getEpoch(short_code=self.short_code)
      return order
    except Exception as e:
      if "Too many requests" in str(e):
        logging.info('%s:%s retrying order cancellation in 1 s for %s', self.broker, self.short_code, order.orderId)
        import time
        time.sleep(1)
        self.cancelOrder(order)
      logging.info('%s:%s Order cancel failed: %s', self.broker, self.short_code, str(e))
      raise Exception(str(e))

  def fetchAndUpdateAllOrderDetails(self, orders):
    logging.info('%s:%s Going to fetch order book', self.broker, self.short_code)
    kite = self.brokerHandle
    orderBook = None
    try:
      orderBook = kite.orders()
    except Exception as e:
      logging.error('%s:%s Failed to fetch order book', self.broker, self.short_code)
      return []

    logging.debug('%s:%s Order book length = %d', self.broker, self.short_code, len(orderBook))
    numOrdersUpdated = 0
    missingOrders = []
    
    for bOrder in orderBook:
      foundOrder = None
      foundChildOrder = None
      parentOrder = None
      for order in orders.keys():
        if order.orderId == bOrder['order_id']:
          foundOrder = order
        if order.orderId == bOrder['parent_order_id']:
          foundChildOrder = bOrder
          parentOrder = order
          
      if foundOrder != None:
        logging.debug('Found order for orderId %s', foundOrder.orderId)
        foundOrder.qty = bOrder['quantity']
        foundOrder.filledQty = bOrder['filled_quantity']
        foundOrder.pendingQty = bOrder['pending_quantity']
        foundOrder.orderStatus = bOrder['status']
        if foundOrder.orderStatus == OrderStatus.CANCELLED and foundOrder.filledQty > 0:
          # Consider this case as completed in our system as we cancel the order with pending qty when strategy stop timestamp reaches
          foundOrder.orderStatus = OrderStatus.COMPLETE
        foundOrder.price = bOrder['price']
        foundOrder.triggerPrice = bOrder['trigger_price']
        foundOrder.averagePrice = bOrder['average_price']
        foundOrder.lastOrderUpdateTimestamp = bOrder['exchange_update_timestamp']
        logging.debug('%s:%s:%s Updated order %s', self.broker, self.short_code, orders[foundOrder], foundOrder)
        numOrdersUpdated += 1
      elif foundChildOrder != None:
        oip = OrderInputParams(parentOrder.tradingSymbol)
        oip.exchange = parentOrder.exchange
        oip.productType = parentOrder.productType
        oip.orderType = parentOrder.orderType
        oip.price = parentOrder.price
        oip.triggerPrice = parentOrder.triggerPrice
        oip.qty = parentOrder.qty
        oip.tag = parentOrder.tag
        oip.productType = parentOrder.productType
        order = Order(oip)
        order.orderId = bOrder['order_id']
        order.parentOrderId = parentOrder.orderId
        order.orderPlaceTimestamp = Utils.getEpoch(short_code=self.short_code) #TODO should get from bOrder
        missingOrders.append(order)
      
    return missingOrders

  def convertToBrokerProductType(self, productType):
    kite = self.brokerHandle
    if productType == ProductType.MIS:
      return kite.PRODUCT_MIS
    elif productType == ProductType.NRML:
      return kite.PRODUCT_NRML
    elif productType == ProductType.CNC:
      return kite.PRODUCT_CNC
    return None 

  def convertToBrokerOrderType(self, orderType):
    kite = self.brokerHandle
    if orderType == OrderType.LIMIT:
      return kite.ORDER_TYPE_LIMIT
    elif orderType == OrderType.MARKET:
      return kite.ORDER_TYPE_MARKET
    elif orderType == OrderType.SL_MARKET:
      return kite.ORDER_TYPE_SLM
    elif orderType == OrderType.SL_LIMIT:
      return kite.ORDER_TYPE_SL
    return None

  def convertToBrokerDirection(self, direction):
    kite = self.brokerHandle
    if direction == Direction.LONG:
      return kite.TRANSACTION_TYPE_BUY
    elif direction == Direction.SHORT:
      return kite.TRANSACTION_TYPE_SELL
    return None
  
  def updateOrder(self, order, data):
    if order is None:
      return
    logging.info(data)
