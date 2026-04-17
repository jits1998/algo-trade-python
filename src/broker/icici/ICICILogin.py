import logging
import urllib
from breeze_connect import BreezeConnect

from config.Config import getSystemConfig
from core.BaseLogin import BaseLogin

from broker.icici.ICICIHandler import ICICIHandler

class ICICILogin(BaseLogin):
  def __init__(self, brokerAppDetails):
    BaseLogin.__init__(self, brokerAppDetails)

  def login(self, args):
    logging.info('==> ICICILogin .args => %s', args);
    systemConfig = getSystemConfig()
    brokerHandle = BreezeConnect(api_key=self.brokerAppDetails.appKey)
    self.setBrokerHandle(ICICIHandler(brokerHandle, self.brokerAppDetails))
    redirectUrl = None
    if 'apisession' in args:
      
      apisession = args['apisession']

      logging.info('ICICI apisession = %s', apisession)

      self.setAccessToken(apisession)
      self.getBrokerHandle().set_access_token(apisession)

      if not brokerHandle.user_id == self.brokerAppDetails.clientID:
        raise Exception("Invalid User Credentials")
      
      logging.info('ICICI Login successful. apisession = %s', apisession)
      
      homeUrl = systemConfig['homeUrl'] + '?loggedIn=true'
      logging.info('ICICI Redirecting to home page %s', homeUrl)

      redirectUrl = homeUrl
    else: 
        redirectUrl = "https://api.icicidirect.com/apiuser/login?api_key="+urllib.parse.quote_plus(self.brokerAppDetails.appKey)

    return redirectUrl