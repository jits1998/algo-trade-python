import logging
import time

from kiteconnect import KiteTicker

from core.BaseTicker import BaseTicker
from instruments.Instruments import Instruments
from models.TickData import TickData

class ZerodhaTicker(BaseTicker):
  def __init__(self, short_code):
    super().__init__("zerodha", short_code)

  def startTicker(self, appKey, accessToken):
    if accessToken == None:
      logging.error('ZerodhaTicker startTicker: Cannot start ticker as accessToken is empty')
      return
    
    ticker = KiteTicker(appKey, accessToken)
    ticker.on_connect = self.on_connect
    ticker.on_close = self.on_close
    ticker.on_error = self.on_error
    ticker.on_reconnect = self.on_reconnect
    ticker.on_noreconnect = self.on_noreconnect
    ticker.on_ticks = self.on_ticks
    ticker.on_order_update = self.on_order_update

    logging.info('ZerodhaTicker: Going to connect..')
    self.ticker = ticker
    self.ticker.connect(threaded=True)

    # sleep for 2 seconds for ticker connection establishment
    while self.ticker.ws is None:
      logging.warning('Waiting for ticker connection establishment..')
      time.sleep(2)

  def stopTicker(self):
    logging.info('ZerodhaTicker: stopping..')
    self.ticker.close(1000, "Manual close")

  def registerSymbols(self, symbols, mode = KiteTicker.MODE_QUOTE):
    tokens = []
    for symbol in symbols:
      isd = Instruments.getInstrumentDataBySymbol(self.short_code, symbol)
      token = isd['instrument_token']
      logging.debug('ZerodhaTicker registerSymbol: %s token = %s', symbol, token)
      tokens.append(token)

    logging.debug('ZerodhaTicker Subscribing tokens %s', tokens)
    self.ticker.subscribe(tokens)
    self.ticker.set_mode(mode, tokens)

  def unregisterSymbols(self, symbols):
    tokens = []
    for symbol in symbols:
      isd = Instruments.getInstrumentDataBySymbol(self.short_code, symbol)
      token = isd['instrument_token']
      logging.debug('ZerodhaTicker unregisterSymbols: %s token = %s', symbol, token)
      tokens.append(token)

    logging.info('ZerodhaTicker Unsubscribing tokens %s', tokens)
    self.ticker.unsubscribe(tokens)

  def getRegisteredSymbols(self):
    """Returns list of currently subscribed tokens from the ticker"""
    return self.ticker.subscribed_tokens

  def on_ticks(self, ws, brokerTicks):
    # convert broker specific Ticks to our system specific Ticks (models.TickData) and pass to super class function
    ticks = []
    for bTick in brokerTicks:
      isd = Instruments.getInstrumentDataByToken(self.short_code, bTick['instrument_token'])
      tradingSymbol = isd['tradingsymbol']
      tick = TickData(tradingSymbol)
      tick.lastTradedPrice = bTick['last_price']
      if not isd['segment'] == "INDICES":
        tick.lastTradedQuantity = bTick['last_traded_quantity']
        tick.avgTradedPrice = bTick['average_traded_price']
        tick.volume = bTick['volume_traded']
        tick.totalBuyQuantity = bTick['total_buy_quantity']
        tick.totalSellQuantity = bTick['total_sell_quantity']
      else:
        tick.exchange_timestamp = bTick['exchange_timestamp']
      tick.open = bTick['ohlc']['open']
      tick.high = bTick['ohlc']['high']
      tick.low = bTick['ohlc']['low']
      tick.close = bTick['ohlc']['close']
      tick.change = bTick['change']
      ticks.append(tick)
      
    self.onNewTicks(ticks)

  def on_connect(self, ws, response):
    self.onConnect()

  def on_close(self, ws, code, reason):
    self.onDisconnect(code, reason)

  def on_error(self, ws, code, reason):
    self.onError(code, reason)

  def on_reconnect(self, ws, attemptsCount):
    self.onReconnect(attemptsCount)

  def on_noreconnect(self, ws):
    self.onMaxReconnectsAttempt()

  def on_order_update(self, ws, data):
    # {'account_id': 'OZ5207', 'unfilled_quantity': 0, 'checksum': '', 'placed_by': 'OZ5207', 
    #  'order_id': '240322602599135', 'exchange_order_id': '1711099883046875667', 'parent_order_id': None, 
    #  'status': 'CANCELLED', 'status_message': None, 'status_message_raw': None, 'order_timestamp': '2024-03-22 15:05:36', 
    #  'exchange_update_timestamp': '2024-03-22 15:05:36', 'exchange_timestamp': '2024-03-22 15:05:36', 
    #  'variety': 'regular', 'exchange': 'BFO', 'tradingsymbol': 'SENSEX2432272900CE', 
    #  'instrument_token': 217653509, 'order_type': 'LIMIT', 'transaction_type': 'SELL', 'validity': 'DAY', 
    #  'product': 'MIS', 'quantity': 10, 'disclosed_quantity': 0, 'price': 61.75, 'trigger_price': 0, 'average_price': 0, 
    #  'filled_quantity': 0, 'pending_quantity': 10, 'cancelled_quantity': 10, 'market_protection': 0, 'meta': {}, 
    #  'tag': 'atmre.Sensex3pm_15:0', 'tags': ['atmre.Sensex3pm_15:0'], 'guid': '49765X0yt4KAv3tlbu'}
    self.onOrderUpdate(data['order_id'], data)
