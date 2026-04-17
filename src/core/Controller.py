import logging

from config.Config import getBrokerAppConfig
from models.BrokerAppDetails import BrokerAppDetails
from broker.zerodha.ZerodhaLogin import ZerodhaLogin
from broker.icici.ICICILogin import ICICILogin

class Controller:
  brokerLogin = {} # static variable
  brokerName = {} # static variable

  def handleBrokerLogin(args, short_code):
    brokerAppConfig = getBrokerAppConfig(short_code)

    brokerAppDetails = BrokerAppDetails(brokerAppConfig['broker'])
    brokerAppDetails.short_code = short_code
    

    logging.info('handleBrokerLogin appKey %s', brokerAppDetails.appKey)
    Controller.brokerName[short_code] = brokerAppDetails.broker
    if Controller.brokerName[short_code] == 'zerodha':
      brokerAppDetails.setClientID(brokerAppConfig['clientID'])
      brokerAppDetails.setAppKey(brokerAppConfig['appKey'])
      brokerAppDetails.setAppSecret(brokerAppConfig['appSecret'])
      Controller.brokerLogin[short_code] = ZerodhaLogin(brokerAppDetails)
    elif Controller.brokerName[short_code] == 'icici':
      brokerAppDetails.setClientID(brokerAppConfig['clientID'])
      brokerAppDetails.setAppKey(brokerAppConfig['apikey'])
      brokerAppDetails.setAppSecret(brokerAppConfig['apisecret'])
      Controller.brokerLogin[short_code] = ICICILogin(brokerAppDetails)
    # Other brokers - not implemented
    #elif Controller.brokerName == 'fyers':
      #Controller.brokerLogin = FyersLogin(brokerAppDetails)

    redirectUrl = Controller.brokerLogin[short_code].login(args)
    return redirectUrl

  def getBrokerLogin(short_code):
    return Controller.brokerLogin.get(short_code, None)

  def getBrokerName(short_code):
    return Controller.brokerName.get(short_code, None)
