from flask.views import MethodView
from flask import render_template, request, abort, session, redirect, url_for
from utils.Utils import Utils
from config.Config import getBrokerAppConfig, getServerConfig
import json
import os
import logging
from datetime import datetime


class HistoryAPI(MethodView):
  def get(self, short_code):
    # Check authentication
    if not session.get('short_code', None) == short_code or \
       session.get('access_token', None) is None:
      return redirect(url_for('home', short_code=short_code))
    # Get tradeDate parameter from request args
    trade_date = request.args.get("date", None)

    # If no date is provided, show date selection page
    if trade_date is None:
      return render_template('history.html',
                            short_code=short_code,
                            trade_date=None,
                            multiple=getBrokerAppConfig(short_code).get("multiple", 1))

    try:
      # Validate date format and get day of week
      if isinstance(trade_date, str):
        date_obj = datetime.strptime(trade_date, '%Y-%m-%d')
        day_of_week = date_obj.strftime('%A')  # Full day name (e.g., Monday)
    except ValueError:
      return "Error: Invalid date format. Use YYYY-MM-DD", 400

    # Build file paths for the specified date
    serverConfig = getServerConfig()
    baseTradesDir = os.path.join(serverConfig['deployDir'], 'trades')
    tradesDir = os.path.join(baseTradesDir, trade_date)

    # Load strategies data from file
    strategiesFilepath = os.path.join(tradesDir, getBrokerAppConfig(short_code)['broker'] + '_' +
                                      getBrokerAppConfig(short_code)['clientID'] + '_strategies.json')

    # Load trades data from file
    tradesFilepath = os.path.join(tradesDir, getBrokerAppConfig(short_code)['broker'] + '_' +
                                   getBrokerAppConfig(short_code)['clientID'] + '.json')

    # Check if files exist
    if not os.path.exists(strategiesFilepath):
      return render_template('history.html',
                            short_code=short_code,
                            trade_date=trade_date,
                            error=f"No strategies file found for date {trade_date}",
                            multiple=getBrokerAppConfig(short_code).get("multiple", 1))

    if not os.path.exists(tradesFilepath):
      return render_template('history.html',
                            short_code=short_code,
                            trade_date=trade_date,
                            error=f"No trades file found for date {trade_date}",
                            multiple=getBrokerAppConfig(short_code).get("multiple", 1))

    # Load strategies data
    try:
      with open(strategiesFilepath, 'r') as sFile:
        strategies_data = json.load(sFile)
    except Exception as e:
      logging.error(f"Error loading strategies file: {e}")
      return f"Error loading strategies file: {e}", 500

    # Load trades data
    try:
      with open(tradesFilepath, 'r') as tFile:
        trades_data = json.load(tFile)
    except Exception as e:
      logging.error(f"Error loading trades file: {e}")
      return f"Error loading trades file: {e}", 500

    # Group trades by strategy
    strategies_with_trades = {}
    for strategy_name, strategy_info in strategies_data.items():
      strategies_with_trades[strategy_name] = {
        'info': strategy_info,
        'trades': []
      }

    # Assign trades to their strategies
    for trade in trades_data:
      strategy_name = trade.get('strategy', 'Unknown')
      if strategy_name in strategies_with_trades:
        strategies_with_trades[strategy_name]['trades'].append(trade)
      else:
        # Handle trades without a known strategy
        if 'Unknown' not in strategies_with_trades:
          strategies_with_trades['Unknown'] = {
            'info': {'enabled': False},
            'trades': []
          }
        strategies_with_trades['Unknown']['trades'].append(trade)

    # Render the history template
    return render_template('history.html',
                          strategies=strategies_with_trades,
                          trade_date=trade_date,
                          day_of_week=day_of_week,
                          short_code=short_code,
                          multiple=getBrokerAppConfig(short_code).get("multiple", 1))
