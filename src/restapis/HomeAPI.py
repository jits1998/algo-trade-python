import logging

from flask.views import MethodView
from flask import render_template, redirect, jsonify
from flask import request, session, make_response, abort
from utils.Utils import Utils
from core.Controller import Controller
from config.Config import getBrokerAppConfig
from functools import wraps


# Authentication decorator
def token_required(f):
    @wraps(f)
    def decorator(*args, **kwargs):
      short_code = kwargs["short_code"]
      trademanager = Utils.getTradeManager(short_code)
      # ensure the jwt-token is passed with the headers
      if not session.get('short_code', None) == short_code or \
        session.get('access_token', None) is None or trademanager is None:
        abort(404)
      return f(trademanager, *args, **kwargs)
    return decorator


class HomeAPI(MethodView):
  def get(self, short_code):

    if session.get('short_code', None) is not None and not short_code == session['short_code']:
      session.clear()
      session['short_code'] = short_code

    if session.get('access_token', None) is None and Utils.getTradeManager(short_code) is None:
      session['short_code'] = short_code
      if request.args.get("accessToken",None) is not None:
        session['access_token'] = request.args["accessToken"]
        return redirect(request.path)
      return render_template('index.html', broker = getBrokerAppConfig(short_code).get("broker", "zerodha"))
    else:
      trademanager = Utils.getTradeManager(short_code)
      return render_template('index_algostarted_new.html', algoStarted = trademanager is not None,
                                                        isReady = trademanager is not None and trademanager.isReady,
                                                        multiple = getBrokerAppConfig(short_code).get("multiple", 1),
                                                        short_code = short_code)


def _maybe_start_shadow_run(trademanager):
  """Fire a shadow run in a background thread if data is stale and no run is in progress."""
  import threading
  from datetime import datetime, timedelta

  if not hasattr(trademanager, 'algoInstance'):
    return
  from config.Config import getBrokerAppConfig
  if not getBrokerAppConfig(trademanager.name).get('shadowEnabled', True):
    return
  sm = getattr(trademanager, 'shadowManager', None)
  # Already running
  if sm is not None and sm.is_running:
    return
  # Wait until the live run has at least one trade before shadowing
  if not any(t.tradeState != 'disabled' for t in trademanager.trades):
    return
  # Check staleness: skip if last run completed less than 3 minutes ago
  if sm is not None and sm.completed_at is not None:
    if (datetime.now() - sm.completed_at).total_seconds() < 180:
      return

  def _run_shadow():
    from trademgmt.ShadowManager import ShadowManager
    short_code = trademanager.name
    try:
      new_sm = ShadowManager(short_code, trademanager.algoInstance, trademanager)
      # Set is_running immediately so any concurrent getState call sees it
      new_sm.is_running = True
      trademanager.shadowManager = new_sm
      new_sm.setupBacktestEnvironment()
      new_sm.loadIndexHistoricalData()
      if not new_sm.timestamp_maps.get('NIFTY 50'):
        logging.warning('ShadowManager: No NIFTY 50 data for today, skipping')
        new_sm.is_running = False
        return
      first_ts = min(new_sm.timestamp_maps['NIFTY 50'].keys())
      first_candle = new_sm.timestamp_maps['NIFTY 50'][first_ts]
      new_sm.symbolToCMPMap['exchange_timestamp'] = first_candle['date']
      new_sm.symbolToCMPMap['NIFTY 50'] = first_candle['close']
      trademanager.algoInstance.startStrategies(short_code, trademanager.multiple, new_sm)
      new_sm.run()
    except Exception:
      logging.exception('ShadowManager: Shadow run failed')
      if trademanager.shadowManager is not None:
        trademanager.shadowManager.is_running = False

  threading.Thread(target=_run_shadow, name=f'{trademanager.name}_shadow', daemon=True).start()


def getState(short_code):
  from datetime import datetime
  trademanager = Utils.getTradeManager(short_code)
  if trademanager is None:
    return jsonify({
      'strategies': [], 'ltps': {}, 'margins': {}, 'orders': [],
      'positions': [], 'highestMarginUsed': 0, 'isReady': False,
    })

  ltps = trademanager.symbolToCMPMap
  broker_login = Controller.getBrokerLogin(short_code)
  broker = broker_login.getBrokerHandle() if broker_login is not None else None

  def fmt_ts(ts):
    if ts is None:
      return None
    if isinstance(ts, str):
      return ts
    if isinstance(ts, (int, float)):
      return datetime.fromtimestamp(ts).strftime("%H:%M:%S")
    return ts.strftime("%H:%M:%S")

  strategies_data = []
  for strategy in trademanager.strategyToInstanceMap.values():
    trades_data = []
    for trade in sorted(strategy.trades, key=lambda t: t.startTimestamp or 0):
      if trade.tradeState == 'disabled':
        continue
      trades_data.append({
        'tradeID': trade.tradeID,
        'tradingSymbol': trade.tradingSymbol,
        'direction': trade.direction,
        'filledQty': trade.filledQty,
        'entry': trade.entry,
        'exit': trade.exit,
        'cmp': trade.cmp,
        'pnl': trade.pnl,
        'stopLoss': trade.stopLoss,
        'requestedEntry': trade.requestedEntry,
        'startTimestamp': fmt_ts(trade.startTimestamp),
        'endTimestamp': fmt_ts(trade.endTimestamp),
        'exitReason': trade.exitReason,
        'tradeState': trade.tradeState,
        'slWhiskerHit': trade.slWhiskerHit,
      })
    strategies_data.append({
      'name': strategy.getName(),
      'lots': strategy.getLots(),
      'enabled': strategy.isEnabled(),
      'paused': strategy.getName() in trademanager.pausedStrategies,
      'target': strategy.strategyTarget * strategy.getLots(),
      'sl': strategy.strategySL * strategy.getLots(),
      'symbol': getattr(strategy, 'symbol', None),
      'trades': trades_data,
    })

  exchange_ts = ltps.get('exchange_timestamp')
  exchange_ts_str = exchange_ts.strftime('%Y-%m-%d %H:%M:%S') if exchange_ts else None
  exchange_ts_display = exchange_ts.strftime('%A, %b %d %Y %X') if exchange_ts else None
  exchange_ts_mobile = exchange_ts.strftime('%a, %b %d %H:%M') if exchange_ts else None

  authenticated = session.get('short_code') == short_code and session.get('access_token') is not None

  if authenticated:
    _maybe_start_shadow_run(trademanager)

  margins = broker.margins() if (broker and authenticated) else {}
  positions = broker.positions() if broker else {}
  orders = broker.orders() if broker else {}

  open_orders = []
  for order in orders:
    if order.get('status') not in ('COMPLETE', 'CANCELLED'):
      open_orders.append({
        'tag': order.get('tag'),
        'status': order.get('status'),
        'tradingsymbol': order.get('tradingsymbol'),
        'order_type': order.get('order_type'),
        'transaction_type': order.get('transaction_type'),
        'pending_quantity': order.get('pending_quantity'),
        'quantity': order.get('quantity'),
        'ltp': ltps.get(order.get('tradingsymbol'), 'Unknown'),
        'price': order.get('price'),
        'trigger_price': order.get('trigger_price'),
      })

  mis_positions = []
  for pos in positions.get('day', []):
    if pos.get('product') == 'MIS':
      mis_positions.append({
        'tradingsymbol': pos.get('tradingsymbol'),
        'quantity': pos.get('quantity'),
      })

  shadow_deviations = []
  shadow_last_run = None
  shadow_status = 'idle'
  shadow_trades = []
  sm = getattr(trademanager, 'shadowManager', None)
  if sm is not None:
    if sm.is_running:
      shadow_status = 'running'
    else:
      shadow_deviations = sm.deviations
      shadow_last_run = sm.completed_at.strftime('%H:%M:%S') if sm.completed_at else None
      shadow_status = 'ok'
      shadow_trades = [
        {
          'tradeID': t.tradeID,
          'strategy': t.strategy,
          'tradingSymbol': t.tradingSymbol,
          'direction': t.direction,
          'filledQty': t.filledQty,
          'entry': t.entry,
          'exit': t.exit,
          'stopLoss': t.stopLoss,
          'pnl': t.pnl,
          'tradeState': t.tradeState,
          'exitReason': t.exitReason,
        }
        for t in sm.trades
        if t.tradeState != 'disabled'
      ]
      logging.info('ShadowManager: getState returning %d shadow trades (total in sm.trades: %d)', len(shadow_trades), len(sm.trades))

  return jsonify({
    'strategies': strategies_data,
    'ltps': {
      'NIFTY 50': ltps.get('NIFTY 50', 0),
      'NIFTY BANK': ltps.get('NIFTY BANK', 0),
      'SENSEX': ltps.get('SENSEX', 0),
      'INDIA VIX': ltps.get('INDIA VIX', 0),
      'exchange_timestamp': exchange_ts_str,
      'exchange_timestamp_display': exchange_ts_display,
      'exchange_timestamp_mobile': exchange_ts_mobile,
    },
    'margins': margins,
    'orders': open_orders,
    'positions': mis_positions,
    'highestMarginUsed': trademanager.highestMarginUsed,
    'dayHighestPnl': trademanager.dayHighestPnl,
    'dayLowestPnl': trademanager.dayLowestPnl,
    'isReady': trademanager.isReady,
    'shadowDeviations': shadow_deviations,
    'shadowLastRun': shadow_last_run,
    'shadowStatus': shadow_status,
    'shadowTrades': shadow_trades,
  })
