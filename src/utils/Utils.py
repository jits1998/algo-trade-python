import calendar
import logging
import math
import threading
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta

import psycopg2
from py_vollib.black.greeks.analytical import delta, gamma, rho, theta, vega
from py_vollib.black.implied_volatility import implied_volatility

from config.Config import getHolidays, getSpecialTradingDays
from database import get_connection
from models.Direction import Direction
from ordermgmt.Order import Order
from trademgmt.Trade import Trade
from trademgmt.TradeState import TradeState


class Utils:
  dateFormat = "%Y-%m-%d"
  timeFormat = "%H:%M:%S"
  dateTimeFormat = "%Y-%m-%d %H:%M:%S"

  @staticmethod
  def roundOff(price): # Round off to 2 decimal places
    return round(price, 2)
    
  @staticmethod
  def roundToNSEPrice(price, tick_size = 0.05):
    return max(round(tick_size * math.ceil(price/tick_size), 2),0.05) if price != 0 else 0

  @staticmethod
  def isMarketOpen(datetimeObj=None):
    now = datetimeObj or Utils.getExchangeTimestamp()
    if Utils.isTodayHoliday(now):
      return False
    marketStartTime = Utils.getMarketStartTime(now)
    marketEndTime = Utils.getMarketEndTime(now)
    return now >= marketStartTime and now <= marketEndTime

  @staticmethod
  def isMarketClosedForTheDay(datetimeObj=None):
    # This method returns true if the current time is > marketEndTime
    # Please note this will not return true if current time is < marketStartTime on a trading day
    now = datetimeObj or Utils.getExchangeTimestamp()
    if Utils.isTodayHoliday(now):
      return True
    marketEndTime = Utils.getMarketEndTime(now)
    return now > marketEndTime

  @staticmethod
  def waitTillMarketOpens(context):
    waitSeconds = 1
    while waitSeconds > 0:
      nowEpoch = Utils.getEpoch(Utils.getExchangeTimestamp())
      marketStartTimeEpoch = Utils.getEpoch(Utils.getMarketStartTime())
      waitSeconds = marketStartTimeEpoch - nowEpoch
      time.sleep(1)

  @staticmethod
  def getEpoch(datetimeObj = None, short_code = None):
    # This method converts given datetimeObj to epoch seconds
    if datetimeObj == None:
      datetimeObj = Utils.getExchangeTimestamp(short_code)
    epochSeconds = datetime.timestamp(datetimeObj)
    return int(epochSeconds) # converting double to long

  @staticmethod
  def getMarketStartTime(dateTimeObj = None):
    return Utils.getTimeOfDay(9, 15, 0, dateTimeObj)

  @staticmethod
  def getMarketEndTime(dateTimeObj = None):
    return Utils.getTimeOfDay(15, 30, 0, dateTimeObj)

  @staticmethod
  def getTimeOfDay(hours, minutes, seconds, dateTimeObj = None):
    if dateTimeObj == None:
      dateTimeObj = Utils.getExchangeTimestamp()
    dateTimeObj = dateTimeObj.replace(hour=hours, minute=minutes, second=seconds, microsecond=0)
    return dateTimeObj

  @staticmethod
  def getTimeOfToDay(hours, minutes, seconds, dateTimeObj=None):
    return Utils.getTimeOfDay(hours, minutes, seconds, dateTimeObj)

  @staticmethod
  def getTodayDateStr(datetimeObj=None):
    return Utils.convertToDateStr(datetimeObj or Utils.getExchangeTimestamp())

  @staticmethod
  def convertToDateStr(datetimeObj):
    return datetimeObj.strftime(Utils.dateFormat)

  @staticmethod
  def isHoliday(datetimeObj, ignoreSpecialTradingDays=False):
    dateStr = Utils.convertToDateStr(datetimeObj)
    if not ignoreSpecialTradingDays and dateStr in getSpecialTradingDays():
      return False

    dayOfWeek = calendar.day_name[datetimeObj.weekday()]
    if dayOfWeek == 'Saturday' or dayOfWeek == 'Sunday':
      return True

    if dateStr in getHolidays():
      return True

    return False

  @staticmethod
  def isTodayHoliday(datetimeObj=None):
    return Utils.isHoliday(datetimeObj or Utils.getExchangeTimestamp())
    
  @staticmethod
  def generateTradeID():
    return str(uuid.uuid4())

  @staticmethod
  def calculateTradePnl(trade):
    if trade.tradeState == TradeState.ACTIVE:
      if trade.cmp > 0:
        if trade.direction == Direction.LONG:
          trade.pnl = Utils.roundOff(trade.filledQty * (trade.cmp - trade.entry))
        else:  
          trade.pnl = Utils.roundOff(trade.filledQty * (trade.entry - trade.cmp))
    else:
      if trade.exit > 0:
        if trade.direction == Direction.LONG:
          trade.pnl = Utils.roundOff(trade.filledQty * (trade.exit - trade.entry))
        else:  
          trade.pnl = Utils.roundOff(trade.filledQty * (trade.entry - trade.exit))
    tradeValue = trade.entry * trade.filledQty
    if tradeValue > 0:
      trade.pnlPercentage = Utils.roundOff(trade.pnl * 100 / tradeValue)

    return trade

  @staticmethod
  def prepareMonthlyExpiryFuturesSymbol(inputSymbol, expiryDay=3, datetimeObj=None):
    now = datetimeObj or Utils.getExchangeTimestamp()
    expiryDateTime = Utils.getMonthlyExpiryDayDate(
        datetimeObj=now, expiryDay=expiryDay)
    expiryDateMarketEndTime = Utils.getMarketEndTime(expiryDateTime)
    if now > expiryDateMarketEndTime:
      # increasing today date by 20 days to get some day in next month passing to getMonthlyExpiryDayDate()
      expiryDateTime = Utils.getMonthlyExpiryDayDate(now + timedelta(days=20),expiryDay)
    year2Digits = str(expiryDateTime.year)[2:]
    monthShort = calendar.month_name[expiryDateTime.month].upper()[0:3]
    futureSymbol = inputSymbol + year2Digits + monthShort + 'FUT'
    logging.info('prepareMonthlyExpiryFuturesSymbol[%s] = %s', inputSymbol, futureSymbol)  
    return futureSymbol
  
  @staticmethod
  def prepareMonthlyOptionsSymbol(inputSymbol, strike, optionType, numWeeksPlus = 0, expiryDay = 3):
    expiryDateTime = Utils.getMonthlyExpiryDayDate(expiryDay=expiryDay)
    year2Digits = str(expiryDateTime.year)[2:]
    monthShort = calendar.month_name[expiryDateTime.month].upper()[0:3]
    optionSymbol = inputSymbol + str(year2Digits) + monthShort + str(strike) + optionType.upper()
    return optionSymbol


  @staticmethod
  def prepareWeeklyOptionsSymbol(inputSymbol, strike, optionType, numWeeksPlus = 0, expiryDay = 3):
    datetimeObj = None

    expiryDateTime = Utils.getWeeklyExpiryDayDate(
        inputSymbol, expiryDay=expiryDay, dateTimeObj=datetimeObj)
    # Check if monthly and weekly expiry same
    expiryDateTimeMonthly = Utils.getMonthlyExpiryDayDate(
        expiryDay=expiryDay, datetimeObj=datetimeObj)
    weekAndMonthExpriySame = False
    if expiryDateTime == expiryDateTimeMonthly or expiryDateTimeMonthly == Utils.getTimeOfDay(0, 0, 0, Utils.getExchangeTimestamp()):
      expiryDateTime = expiryDateTimeMonthly
      weekAndMonthExpriySame = True
      logging.debug('Weekly and Monthly expiry is same for %s', expiryDateTime)

    todayMarketStartTime = Utils.getMarketStartTime()
    expiryDayMarketEndTime = Utils.getMarketEndTime(expiryDateTime)
    if numWeeksPlus > 0:
      expiryDateTime = expiryDateTime + timedelta(days=numWeeksPlus * 7)
      expiryDateTime = Utils.getWeeklyExpiryDayDate(inputSymbol, expiryDateTime, expiryDay)
    if todayMarketStartTime > expiryDayMarketEndTime:
      expiryDateTime = expiryDateTime + timedelta(days=6)
      expiryDateTime = Utils.getWeeklyExpiryDayDate(inputSymbol, expiryDateTime, expiryDay)
    
    year2Digits = str(expiryDateTime.year)[2:]
    optionSymbol = None
    if weekAndMonthExpriySame == True:
      monthShort = calendar.month_name[expiryDateTime.month].upper()[0:3]
      optionSymbol = inputSymbol + str(year2Digits) + monthShort + str(strike) + optionType.upper()
    else:
      m = expiryDateTime.month
      d = expiryDateTime.day
      mStr = str(m)
      if m == 10:
        mStr = "O"
      elif m == 11:
        mStr = "N"
      elif m == 12:
        mStr = "D"
      dStr = ("0" + str(d)) if d < 10 else str(d)
      optionSymbol = inputSymbol + str(year2Digits) + mStr + dStr + str(strike) + optionType.upper()
    # logging.info('prepareWeeklyOptionsSymbol[%s, %d, %s, %d] = %s', inputSymbol, strike, optionType, numWeeksPlus, optionSymbol)  
    return optionSymbol

  @staticmethod
  def getStrikeFromSymbol(symbol):
    return int(symbol[-7:-2])

  @staticmethod
  def getTypeFromSymbol(symbol):
    return symbol[-2:]

  @staticmethod
  def getMonthlyExpiryDayDate(datetimeObj = None, expiryDay = 3):
    if datetimeObj == None:
      datetimeObj = Utils.getExchangeTimestamp()
    year = datetimeObj.year
    month = datetimeObj.month
    lastDay = calendar.monthrange(year, month)[1] # 2nd entry is the last day of the month
    datetimeExpiryDay = datetimeObj.replace(day=lastDay)
    while datetimeExpiryDay.weekday() != expiryDay:
      datetimeExpiryDay = datetimeExpiryDay - timedelta(days=1)
    while Utils.isHoliday(datetimeExpiryDay) == True:
      datetimeExpiryDay = datetimeExpiryDay - timedelta(days=1)

    datetimeExpiryDay = Utils.getTimeOfDay(0, 0, 0, datetimeExpiryDay)

    # If calculated expiry is in the past, get next month's expiry
    if datetimeExpiryDay < Utils.getTimeOfDay(0, 0, 0, datetimeObj):
      nextMonthDate = datetimeObj.replace(day=lastDay) + timedelta(days=1)
      return Utils.getMonthlyExpiryDayDate(nextMonthDate, expiryDay)

    return datetimeExpiryDay

  @staticmethod
  def getWeeklyExpiryDayDate(inputSymbol, dateTimeObj = None, expiryDay = 3):

    if dateTimeObj == None:
        dateTimeObj = Utils.getExchangeTimestamp()

    expiryDateTimeMonthly = Utils.getMonthlyExpiryDayDate(
        datetimeObj=dateTimeObj, expiryDay=expiryDay)
    
    if expiryDateTimeMonthly == Utils.getTimeOfDay(0, 0, 0, dateTimeObj):  
      datetimeExpiryDay = expiryDateTimeMonthly
    else:
      
      daysToAdd = 0
      if dateTimeObj.weekday() > expiryDay:
        daysToAdd = 7 - (dateTimeObj.weekday() - expiryDay)
      else:
        daysToAdd = expiryDay - dateTimeObj.weekday()
      datetimeExpiryDay = dateTimeObj + timedelta(days=daysToAdd)
      while Utils.isHoliday(datetimeExpiryDay) == True:
        datetimeExpiryDay = datetimeExpiryDay - timedelta(days=1)

      datetimeExpiryDay = Utils.getTimeOfDay(0, 0, 0, datetimeExpiryDay)
    
    return datetimeExpiryDay

  @staticmethod
  def isTodayWeeklyExpiryDay(inputSymbol, expiryDay = 3):
    expiryDate = Utils.getWeeklyExpiryDayDate(inputSymbol, expiryDay=expiryDay)
    todayDate = Utils.getTimeOfToDay(0, 0, 0)
    if expiryDate == todayDate:
      return True
    return False
  
  @staticmethod
  def isTodayMonthlyExpiryDay(inputSymbol, expiryDay = 3):
    expiryDate = Utils.getMonthlyExpiryDayDate(expiryDay=expiryDay)
    todayDate = Utils.getTimeOfToDay(0, 0, 0)
    if expiryDate == todayDate:
      return True
    return False

  @staticmethod
  def isTodayOneDayBeforeWeeklyExpiryDay(inputSymbol, expiryDay = 3):
    return Utils.findNumberOfDaysBeforeWeeklyExpiryDay(inputSymbol, expiryDay) == 1

  @staticmethod
  def findNumberOfDaysBeforeWeeklyExpiryDay(inputSymbol, expiryDay = 3, dateTimeObj = None):
    if dateTimeObj is None:
      dateTimeObj = Utils.getTimeOfToDay(0, 0, 0)

    expiryDate = Utils.getWeeklyExpiryDayDate(inputSymbol, dateTimeObj=dateTimeObj, expiryDay=expiryDay)

    if Utils.getTimeOfDay(0, 0, 0, dateTimeObj) == expiryDate:
      return 0

    cur = Utils.getTimeOfDay(0, 0, 0, dateTimeObj)
    currentWeekTradingDates = []

    while cur < expiryDate:

      if Utils.isHoliday(cur, ignoreSpecialTradingDays=True):
        cur += timedelta(days = 1)
        continue

      currentWeekTradingDates.append(cur)
      cur += timedelta(days = 1)
    return len(currentWeekTradingDates)

  @staticmethod
  def findNumberOfDaysBeforeMonthlyExpiryDay(expiryDay = 3, dateTimeObj = None):
    if dateTimeObj is None:
      dateTimeObj = Utils.getTimeOfToDay(0, 0, 0)

    expiryDate = Utils.getMonthlyExpiryDayDate(datetimeObj=dateTimeObj, expiryDay=expiryDay)

    if Utils.getTimeOfDay(0, 0, 0, dateTimeObj) == expiryDate:
      return 0

    cur = Utils.getTimeOfDay(0, 0, 0, dateTimeObj)
    tradingDates = []

    while cur < expiryDate:
      if Utils.isHoliday(cur, ignoreSpecialTradingDays=True):
        cur += timedelta(days = 1)
        continue
      tradingDates.append(cur)
      cur += timedelta(days = 1)
    return len(tradingDates)

  @staticmethod
  def getNearestStrikePrice(price, nearestMultiple = 50):
    return round(price / nearestMultiple) * nearestMultiple
    
  @staticmethod
  def getOrderStrength(quote, direction):
    if direction == Direction.SHORT:
      return quote.totalSellQuantity / quote.totalBuyQuantity
    else:
      return quote.totalBuyQuantity / quote.totalSellQuantity
    
  @staticmethod
  def getVIXAdjustment(short_code):
    return math.pow(Utils.getTradeManager(short_code).symbolToCMPMap["INDIA VIX"]/16, 0.5)

  @staticmethod
  def getUnderlyingBasedSL(inputSymbol, underLyingPrice, strikePrice, quote, percentageUnderlying, type, expiryDay=2):
    percentageUnderlying = (1 + 0/100) * percentageUnderlying #adjust for vix
    greeks = Utils.greeks(quote, Utils.getWeeklyExpiryDayDate(inputSymbol, expiryDay = expiryDay), underLyingPrice, strikePrice, 0.1, type)
    return underLyingPrice*abs(greeks['Delta'])*percentageUnderlying/100

  @staticmethod
  def greeks(premium, expiry, future_price, strike_price, intrest_rate, instrument_type, datetimeObj=None):
    # t = ((datetime(expiry.year, expiry.month, expiry.day, 15, 30) - datetime(2021, 7, 8, 10, 15, 19))/timedelta(days=1))/365
    exchange_ts = datetimeObj or Utils.getExchangeTimestamp()
    expiry_dt = datetime(expiry.year, expiry.month, expiry.day,
                         15, 30).replace(tzinfo=exchange_ts.tzinfo)
    t = ((expiry_dt - exchange_ts)/timedelta(days=1))/365
    F = future_price
    K = strike_price
    r = intrest_rate
    flag = instrument_type[0].lower()
    imp_v = implied_volatility(premium, F, K, r, t, flag)
    return {
            "IV": imp_v,
            "Delta": delta(flag, F, K, t, r, imp_v),
            #"Gamma": gamma(flag, S, K, t, r, imp_v),
            #"Rho": rho(flag, S, K, t, r, imp_v),
            #"Theta": theta(flag, S, K, t, r, imp_v),
            "Vega": vega(flag, F, K, t, r, imp_v)
            }

  @staticmethod
  def getTradeManager(short_code = None):
    if not short_code:
      short_code = Utils.getShortCode()
    for t in threading.enumerate():
      if t.name == short_code:
        return t

  @staticmethod
  def getExchangeTimestamp(short_code=None, tradeManager=None):
    """
    Get the current exchange timestamp from TradeManager's symbolToCMPMap
    This is used for backtesting to use historical timestamps instead of datetime.now()
    For live trading, falls back to datetime.now()
    """
    if tradeManager is None:
      tradeManager = Utils.getTradeManager(short_code)
    if tradeManager and hasattr(tradeManager, 'symbolToCMPMap') and 'exchange_timestamp' in tradeManager.symbolToCMPMap:
      return tradeManager.symbolToCMPMap['exchange_timestamp']
    # Fallback to datetime.now() if exchange_timestamp is not available (for live trading)
    return datetime.now()

  @staticmethod
  def getShortCode():
    if threading.current_thread().name.find("_") > -1:
      return threading.current_thread().name.split("_")[0]
    else:
      return threading.current_thread().name

  @staticmethod
  def convertJSONToOrder(jsonData):
    if jsonData == None:
        return None
    order = Order()
    order.tradingSymbol = jsonData['tradingSymbol']
    order.exchange = jsonData['exchange']
    order.productType = jsonData['productType']
    order.orderType = jsonData['orderType']
    order.price = jsonData['price']
    order.triggerPrice = jsonData['triggerPrice']
    order.qty = jsonData['qty']
    order.orderId = jsonData['orderId']
    order.orderStatus = jsonData['orderStatus']
    order.averagePrice = jsonData['averagePrice']
    order.filledQty = jsonData['filledQty']
    order.pendingQty = jsonData['pendingQty']
    order.orderPlaceTimestamp = jsonData['orderPlaceTimestamp']
    order.lastOrderUpdateTimestamp = jsonData['lastOrderUpdateTimestamp']
    order.message = jsonData['message']
    order.parentOrderId = jsonData.get('parent_order_id','')
    return order

  @staticmethod
  def convertJSONToTrade(jsonData):
    trade = Trade(jsonData['tradingSymbol'])
    trade.tradeID = jsonData['tradeID']
    trade.strategy = jsonData['strategy']
    trade.direction = jsonData['direction']
    trade.productType = jsonData['productType']
    trade.isFutures = jsonData['isFutures']
    trade.isOptions = jsonData['isOptions']
    trade.optionType = jsonData['optionType']
    trade.underLying = jsonData.get('underLying', "")
    trade.placeMarketOrder = jsonData['placeMarketOrder']
    trade.intradaySquareOffTimestamp = jsonData['intradaySquareOffTimestamp']
    trade.requestedEntry = jsonData['requestedEntry']
    trade.entry = jsonData['entry']
    trade.qty = jsonData['qty']
    trade.filledQty = jsonData['filledQty']
    trade.initialStopLoss = jsonData['initialStopLoss']
    trade.stopLoss = jsonData['_stopLoss']
    trade.stopLossPercentage = jsonData.get('stopLossPercentage', 0)
    trade.stopLossUnderlyingPercentage = jsonData.get('stopLossUnderlyingPercentage', 0)
    trade.target = jsonData['target']
    trade.cmp = jsonData['cmp']
    trade.tradeState = jsonData['tradeState']
    trade.timestamp = jsonData['timestamp']
    trade.createTimestamp = jsonData['createTimestamp']
    trade.startTimestamp = jsonData['startTimestamp']
    trade.endTimestamp = jsonData['endTimestamp']
    trade.pnl = jsonData['pnl']
    trade.pnlPercentage = jsonData['pnlPercentage']
    trade.exit = jsonData['exit']
    trade.exitReason = jsonData['exitReason']
    trade.slWhiskerHit = jsonData.get('slWhiskerHit', False)
    trade.tag = jsonData.get('tag', None)
    trade.shadowDeviationStatus = jsonData.get('shadowDeviationStatus', {})
    trade.exchange = jsonData['exchange']
    for entryOrder in jsonData['entryOrder']:
      trade.entryOrder.append(Utils.convertJSONToOrder(entryOrder))
    for slOrder in jsonData['slOrder']:
      trade.slOrder.append(Utils.convertJSONToOrder(slOrder))
    for trargetOrder in jsonData['targetOrder']:
      trade.targetOrder.append(Utils.convertJSONToOrder(trargetOrder))
    return trade

  @staticmethod
  @contextmanager
  def _getQuestDBCursor():
    """Context manager: yields a raw psycopg2 cursor, closes cursor and returns connection to pool on exit."""
    connection = get_connection()
    # Set autocommit on the underlying psycopg2 connection, not the SQLAlchemy wrapper.
    # _ConnectionFairy.autocommit does not propagate to the DBAPI connection,
    # so inserts would silently roll back when the connection is returned to the pool.
    connection.driver_connection.autocommit = True
    cursor = connection.driver_connection.cursor()
    try:
      yield cursor
    finally:
      cursor.close()
      connection.driver_connection.autocommit = False
      connection.close()

  @staticmethod
  def getQuestDBConnection(short_code):
    """Connect to QuestDB and ensure tables exist for short_code. Called once at startup."""
    try:
      with Utils._getQuestDBCursor() as cursor:
        # Create trade tracking tables for this short_code
        cursor.execute('''CREATE TABLE IF NOT EXISTS {0} ( ts TIMESTAMP, strategy string, tradingSymbol string, tradeId string, cmp float, entry float, pnl float, qty int, status string) timestamp(ts) partition by year'''.format(short_code))
        cursor.execute('''CREATE TABLE IF NOT EXISTS {0}_tickData( ts TIMESTAMP, tradingSymbol string, ltp float, qty int, avgPrice float, volume int, totalBuyQuantity int, totalSellQuantity int, open float, high float, low float, close float, change float) timestamp(ts) partition by year'''.format(short_code))

        # Create historical_candles table with DEDUP for caching broker API data
        cursor.execute('''
          CREATE TABLE IF NOT EXISTS historical_candles (
            ts TIMESTAMP,
            trading_symbol VARCHAR,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume LONG,
            oi LONG
          ) timestamp(ts) PARTITION BY DAY WAL
          DEDUP UPSERT KEYS(ts, trading_symbol)
        ''')

      logging.info("Connected to Quest DB")
    except Exception as err:
      logging.error("Can't connect to QuestDB: %s", str(err))

  @staticmethod
  def getHighestPrice(short_code, startTimestamp, endTimestamp, tradingSymbol):
    try:
      query = "select max(ltp) from '{0}_tickData' where ts BETWEEN to_timestamp('{1}', 'yyyy-MM-dd HH:mm:ss') AND to_timestamp('{2}', 'yyyy-MM-dd HH:mm:ss') AND tradingSymbol = '{3}';"\
              .format(short_code, startTimestamp.strftime("%Y-%m-%d %H:%M:%S"), endTimestamp.strftime("%Y-%m-%d %H:%M:%S"), tradingSymbol)
      result = None
      with Utils._getQuestDBCursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchone()
      return result[0]
    except Exception as err:
      logging.info("Unable to fetch data from QuestDB", str(err))
      return None

  def getLowestPrice(short_code, startTimestamp, endTimestamp, tradingSymbol):
    try:
      query = "select min(ltp) from '{0}_tickData' where ts BETWEEN to_timestamp('{1}', 'yyyy-MM-dd HH:mm:ss') AND to_timestamp('{2}', 'yyyy-MM-dd HH:mm:ss') AND tradingSymbol = '{3}';"\
                .format(short_code, startTimestamp.strftime("%Y-%m-%d %H:%M:%S"), endTimestamp.strftime("%Y-%m-%d %H:%M:%S"), tradingSymbol)
      result = None
      with Utils._getQuestDBCursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchone()
      return result[0]
    except Exception as err:
      logging.info("Unable to fetch data from QuestDB", str(err))
      return None
    

