import os
import logging
import json

from config.Config import getServerConfig, getTimestampsData, saveTimestampsData
from core.Controller import Controller
from utils.Utils import Utils

class Instruments:
  instrumentsList = {}
  symbolToInstrumentMap = {}
  tokenToInstrumentMap = {}

  @staticmethod
  def shouldFetchFromServer(short_code):
    timestamps = getTimestampsData(short_code)
    if 'instrumentsLastSavedAt' not in timestamps:
      return True
    lastSavedTimestamp = timestamps['instrumentsLastSavedAt']
    nowEpoch = Utils.getEpoch(short_code=short_code)
    if nowEpoch - lastSavedTimestamp >= 24 * 60* 60:
      logging.info("Instruments: shouldFetchFromServer() returning True as its been 24 hours since last fetch.")
      return True
    return False

  @staticmethod
  def updateLastSavedTimestamp(short_code):
    timestamps = getTimestampsData(short_code)
    timestamps['instrumentsLastSavedAt'] = Utils.getEpoch(short_code=short_code)
    saveTimestampsData(short_code, timestamps)

  @staticmethod
  def loadInstruments(short_code):
    serverConfig = getServerConfig()
    instrumentsFilepath = os.path.join(serverConfig['deployDir'], short_code + '_instruments.json')
    if os.path.exists(instrumentsFilepath) == False:
      logging.warning(
          'Instruments: instrumentsFilepath %s does not exist', instrumentsFilepath)
      return [] # returns empty list

    isdFile = open(instrumentsFilepath, 'r')
    instruments = json.loads(isdFile.read())
    logging.info('Instruments: loaded %d instruments from file %s', len(instruments), instrumentsFilepath)
    return instruments

  @staticmethod
  def saveInstruments(short_code, instruments = []):
    serverConfig = getServerConfig()
    instrumentsFilepath = os.path.join(serverConfig['deployDir'], short_code+'_instruments.json')
    with open(instrumentsFilepath, 'w') as isdFile:
      json.dump(instruments, isdFile, indent=2, default=str)
    logging.info('Instruments: Saved %d instruments to file %s', len(instruments), instrumentsFilepath)
    # Update last save timestamp
    Instruments.updateLastSavedTimestamp(short_code)

  @staticmethod
  def fetchInstrumentsFromServer(short_code):
    instrumentsList = []
    try:
      brokerHandle = Controller.getBrokerLogin(short_code).getBrokerHandle()
      logging.info('Going to fetch instruments from server...')
      instrumentsList = brokerHandle.instruments('NSE')
      instrumentsListFnO = brokerHandle.instruments('NFO')
      intrumentListBSE = brokerHandle.instruments('BSE')
      instrumentsListBFO = brokerHandle.instruments('BFO')
      # Add FnO instrument list to the main list
      instrumentsList.extend(instrumentsListFnO)
      instrumentsList.extend(intrumentListBSE)
      instrumentsList.extend(instrumentsListBFO)
      logging.info('Fetched %d instruments from server.', len(instrumentsList))
    except Exception as e:
      logging.exception("Exception while fetching instruments from server")
      return []
    return instrumentsList

  @staticmethod
  def fetchInstruments(short_code):
    if short_code in Instruments.instrumentsList:
      return Instruments.instrumentsList[short_code]

    instrumentsList = Instruments.loadInstruments(short_code)
    if len(instrumentsList) == 0 or Instruments.shouldFetchFromServer(short_code) == True:
      instrumentsList = Instruments.fetchInstrumentsFromServer(short_code)
      # Save instruments to file locally
      if len(instrumentsList) > 0:
        Instruments.saveInstruments(short_code, instrumentsList)

    if len(instrumentsList) == 0:
      print("Could not fetch/load instruments data. Hence exiting the app.")
      logging.error("Could not fetch/load instruments data. Hence exiting the app.");
      return instrumentsList
    
    Instruments.symbolToInstrumentMap[short_code] = {}
    Instruments.tokenToInstrumentMap[short_code] = {}
    Controller.getBrokerLogin(short_code).getBrokerHandle().instruments = instrumentsList

    try :
      for isd in instrumentsList:
        tradingSymbol = isd['tradingsymbol']
        instrumentToken = isd['instrument_token']
        # logging.info('%s = %d', tradingSymbol, instrumentToken)
        Instruments.symbolToInstrumentMap[short_code][tradingSymbol] = isd
        Instruments.tokenToInstrumentMap[short_code][instrumentToken] = isd
    except Exception as e:
      logging.exception("Exception while fetching instruments from server: %s", str(e))

    logging.info('Fetching instruments done. Instruments count = %d', len(instrumentsList))
    Instruments.instrumentsList[short_code] = instrumentsList # assign the list to static variable
    return instrumentsList

  @staticmethod
  def getInstrumentDataBySymbol(short_code, tradingSymbol):
    return Instruments.symbolToInstrumentMap[short_code].get(tradingSymbol, {})

  @staticmethod
  def getInstrumentDataByToken(short_code, instrumentToken):
    return Instruments.tokenToInstrumentMap[short_code][instrumentToken]