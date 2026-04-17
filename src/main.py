from logging.handlers import TimedRotatingFileHandler
import os
import logging, datetime
from flask import Flask, redirect, session

from flask_session import Session

from config.Config import getServerConfig
from restapis.HomeAPI import HomeAPI, getState
from restapis.HistoryAPI import HistoryAPI
from restapis.BacktestingAPI import BacktestingAPI, BacktestTradesAPI, BacktestHistoryAPI, BacktestStatusAPI, BacktestDeleteAPI
from restapis import ActionsAPI
from restapis.BrokerLoginAPI import BrokerLoginAPI
from restapis.StartAlgoAPI import StartAlgoAPI
from restapis.ChartAPI import ChartAPI

app = Flask(__name__)

app.config["SESSION_TYPE"] = "filesystem"

Session(app)

app.config['PERMANENT_SESSION_LIFETIME'] =  datetime.timedelta(minutes=360)

@app.route("/")
def redirectHome():
  return redirect("/me/"+session.get('short_code',"5207"))

app.add_url_rule("/", 'default_home', redirectHome)
app.add_url_rule("/me/<short_code>", 'home', view_func=HomeAPI.as_view("home_api"))
app.add_url_rule("/history/<short_code>", 'history', view_func=HistoryAPI.as_view("history_api"))
app.add_url_rule("/backtesting/<short_code>", 'backtesting', view_func=BacktestingAPI.as_view("backtesting_api"), methods=["GET", "POST"])
app.add_url_rule("/backtesting/<short_code>/trades", 'backtest_trades', view_func=BacktestTradesAPI.as_view("backtest_trades_api"), methods=["GET"])
app.add_url_rule("/backtesting/<short_code>/history", 'backtest_history', view_func=BacktestHistoryAPI.as_view("backtest_history_api"), methods=["GET"])
app.add_url_rule("/backtesting/<short_code>/status/<run_id>", 'backtest_status', view_func=BacktestStatusAPI.as_view("backtest_status_api"), methods=["GET"])
app.add_url_rule("/backtesting/<short_code>/runs/<run_id>", 'backtest_delete', view_func=BacktestDeleteAPI.as_view("backtest_delete_api"), methods=["DELETE"])
app.add_url_rule("/apis/broker/login/<broker>", view_func=BrokerLoginAPI.as_view("broker_login_api"))
app.add_url_rule("/apis/algo/start", view_func=StartAlgoAPI.as_view("start_algo_api"))
app.add_url_rule("/chart/<short_code>", view_func=ChartAPI.as_view("chart_api"))
app.add_url_rule("/chart/<short_code>/data", view_func=lambda short_code: ChartAPI().get(short_code, data=True))
app.add_url_rule("/me/<short_code>/strategy/exit/<name>", view_func=ActionsAPI.exitStrategy)
app.add_url_rule("/me/<short_code>/trade/exit/<id>", view_func=ActionsAPI.exitTrade)
app.add_url_rule("/me/<short_code>/trade/enter", view_func=ActionsAPI.enterTrade, methods=["POST"])
app.add_url_rule("/me/<short_code>/getQuote", view_func=ActionsAPI.getQuote)
app.add_url_rule("/me/<short_code>/state", view_func=getState)
app.add_url_rule("/me/<short_code>/shadow/approve/<deviation_id>", view_func=ActionsAPI.approveShadowDeviation, methods=["POST"])


def initLoggingConfg(filepath):
  format = "%(asctime)s: %(message)s"
  handler = TimedRotatingFileHandler(filepath, when='midnight')
  handler.setLevel(logging.INFO)
  handler.setFormatter(logging.Formatter(format, datefmt="%Y-%m-%d %H:%M:%S"))
  logging.getLogger().addHandler(handler)

# Execution starts here
serverConfig = getServerConfig()

deployDir = serverConfig['deployDir']
if os.path.exists(deployDir) == False:
  print("Deploy Directory " + deployDir + " does not exist. Exiting the app.")
  exit(-1)

logFileDir = serverConfig['logFileDir']
if os.path.exists(logFileDir) == False:
  print("LogFile Directory " + logFileDir + " does not exist. Exiting the app.")
  exit(-1)

print("Deploy  Directory = " + deployDir)
print("LogFile Directory = " + logFileDir)
initLoggingConfg(logFileDir + "/app.log")

# Set up debug logger
debug_filepath = logFileDir + "/debug.app.log"
debug_handler = TimedRotatingFileHandler(debug_filepath, when='midnight', backupCount=15)
debug_handler.setLevel(logging.DEBUG)
debug_handler.setFormatter(logging.Formatter("%(asctime)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
logging.getLogger().setLevel(logging.DEBUG)
logging.getLogger().addHandler(debug_handler)
logging.info(f"Debug logging enabled: {debug_filepath}")

logging.info('serverConfig => %s', serverConfig)

werkzeugLog = logging.getLogger('werkzeug')
werkzeugLog.setLevel(logging.ERROR)
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('requests').setLevel(logging.WARNING)

# brokerAppConfig = getBrokerAppConfig()
# logging.info('brokerAppConfig => %s', brokerAppConfig)

port = serverConfig['port']

def timectime(s):
  if s is None:
    return None
  if isinstance(s, str):
    s = datetime.datetime.strptime(s, "%Y-%m-%d %H:%M:%S").timestamp()
  return datetime.datetime.fromtimestamp(s).strftime("%H:%M:%S")

app.jinja_env.filters['ctime']= timectime

# app.run(host = '0.0.0.0', port = port, debug=True)



