
from json import JSONEncoder
from core.BaseStrategy import BaseStrategy
import datetime

class TradeEncoder(JSONEncoder):
  def default(self, o):
    if isinstance(o, (datetime.date, datetime.datetime)):
      return o.isoformat()
    if isinstance(o, (BaseStrategy)):
      return o.asDict()
    return o.__dict__