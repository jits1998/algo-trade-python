from flask import session, request, redirect, url_for
from utils.Utils import Utils
from trademgmt.TradeExitReason import TradeExitReason 
from restapis.HomeAPI import token_required
from strategies.ManualStrategy import ManualStrategy
from instruments.Instruments import Instruments

@token_required
def exitStrategy(trademanager, short_code, name):

  trademanager.squareOffStrategy(trademanager.strategyToInstanceMap[name], TradeExitReason.MANUAL_EXIT)

  return redirect(url_for("home", short_code = short_code))


@token_required
def exitTrade(trademanager, short_code, id):
  name = id.rsplit(":",1)[0]

  trades = trademanager.getAllTradesByStrategy(name)

  for trade in trades:
    if trade.tradeID == id:
      trademanager.squareOffTrade(trade, TradeExitReason.MANUAL_EXIT)
  
  return redirect(url_for("home", short_code = short_code))

@token_required
def enterTrade(trademanager, short_code):
  ul = request.form["index"]
  strike = request.form["strike"]
  iType = request.form["type"]
  trigger = float(request.form["trigger"])
  sl = float(request.form["sl"])
  price = float(request.form["price"])
  target = float(request.form["target"])
  direction = request.form["direction"]
  quantity = request.form["qty"]
  strategyName = request.form.get("strategyName", "ManualStrategy")

  from core.BaseStrategy import BaseStrategy
  symbolConfig = BaseStrategy.SYMBOL_CONFIG[ul]
  expiryDay = symbolConfig["expiryDay"]
  expiryType = symbolConfig["expiryType"]
  exchange = symbolConfig["exchange"]

  strategy = trademanager.strategyToInstanceMap.get(strategyName) or ManualStrategy.getInstance(short_code=short_code)
  strategy.exchange = exchange

  if expiryType == "weekly":
    tradingSymbol = Utils.prepareWeeklyOptionsSymbol(ul, strike, iType, expiryDay=expiryDay)
  else:
    tradingSymbol = Utils.prepareMonthlyOptionsSymbol(ul, strike, iType, expiryDay=expiryDay)

  isd = Instruments.getInstrumentDataBySymbol(short_code, tradingSymbol) # Get instrument data to know qty per lot
  numLots = int(quantity) // isd['lot_size']

  strategy.generateTrade(tradingSymbol, direction, numLots, (trigger if trigger > 0 else price), slPrice=sl, targetPrice=target, placeMarketOrder=(False if trigger > 0 else True))

  return redirect(url_for("home", short_code = short_code))

@token_required
def approveShadowDeviation(trademanager, short_code, deviation_id):
  """
  Approve a single shadow deviation by its deviation_id.
  - Removes it from shadowManager.deviations.
  - If the deviation type is SL_TIGHTER and expand_sl=true is passed, widens
    the live trade's SL order to the shadow value.
  - If the deviation type is STRIKE_MISMATCH and fix_strike=true is passed,
    resyncs ATMStrike/ATMCESymbol/ATMPESymbol/ATMCEQuote/ATMPEQuote on the live
    strategy from the shadow strategy so subsequent re-entries use the correct strike.
  - If no pausing deviations remain for the strategy, unpauses it.
  """
  from flask import jsonify
  from ordermgmt.OrderModifyParams import OrderModifyParams
  from trademgmt.TradeState import TradeState
  from models.Direction import Direction
  from utils.Utils import Utils

  sm = getattr(trademanager, 'shadowManager', None)
  if sm is None:
    return jsonify({'ok': False, 'error': 'no shadow run yet'}), 400

  deviation = next((d for d in sm.deviations if d.get('deviation_id') == deviation_id), None)
  if deviation is None:
    return jsonify({'ok': False, 'error': 'deviation not found'}), 404

  from trademgmt.ShadowDeviation import DeviationType, DeviationStatus

  expand_sl = request.args.get('expand_sl', 'false').lower() == 'true'
  fix_strike = request.args.get('fix_strike', 'false').lower() == 'true'
  ignore = request.args.get('ignore', 'false').lower() == 'true'
  strategy_name = deviation['strategy']
  dev_type = deviation['type']

  # Ignore: mark the trade so shadow won't re-flag it; then fall through to removal
  if ignore and dev_type in (DeviationType.STRIKE_MISMATCH, DeviationType.EXTRA_IN_LIVE):
    import logging
    live_trade_id = deviation.get('live_trade_id')
    live_symbol = deviation.get('live_symbol')
    matched_trade = None
    if live_trade_id:
      matched_trade = next((t for t in trademanager.trades if t.tradeID == live_trade_id), None)
    if matched_trade is None and live_symbol:
      matched_trade = next((t for t in trademanager.trades if t.tradingSymbol == live_symbol and t.strategy == strategy_name), None)
    if matched_trade is not None:
      matched_trade.shadowDeviationStatus[dev_type] = DeviationStatus.IGNORED
      logging.info('Shadow ignore: marked %s as ignored on trade %s', dev_type, matched_trade.tradeID)
    else:
      logging.warning('Shadow ignore: could not find live trade for %s deviation (id=%s symbol=%s)', dev_type, live_trade_id, live_symbol)

  # Resync strike symbols on the live strategy from shadow (only valid for STRIKE_MISMATCH)
  if fix_strike and dev_type == DeviationType.STRIKE_MISMATCH:
    import logging
    live_strategy = trademanager.strategyToInstanceMap.get(strategy_name)
    shadow_strategy = sm.strategyToInstanceMap.get(strategy_name)
    if live_strategy is None or shadow_strategy is None:
      return jsonify({'ok': False, 'error': 'strategy not found in live or shadow'}), 400
    live_strategy.ATMStrike = shadow_strategy.ATMStrike
    live_strategy.ATMCESymbol = shadow_strategy.ATMCESymbol
    live_strategy.ATMPESymbol = shadow_strategy.ATMPESymbol
    live_strategy.ATMCEQuote = shadow_strategy.ATMCEQuote
    live_strategy.ATMPEQuote = shadow_strategy.ATMPEQuote
    logging.info(
      'Shadow approval: Resynced strike on %s — ATMStrike=%s CE=%s PE=%s',
      strategy_name, live_strategy.ATMStrike, live_strategy.ATMCESymbol, live_strategy.ATMPESymbol,
    )
    # Mark the live trade so shadow re-runs don't re-flag this mismatch for the same trade
    live_symbol = deviation.get('live_symbol')
    for trade in trademanager.trades:
      if trade.strategy == strategy_name and trade.tradingSymbol == live_symbol:
        trade.shadowDeviationStatus[DeviationType.STRIKE_MISMATCH] = DeviationStatus.HANDLED
        break

  # Expand SL on the live trade if requested (only valid for SL_TIGHTER)
  if expand_sl and dev_type == DeviationType.SL_TIGHTER:
    shadow_sl = deviation.get('shadow_sl')
    option_type = None
    # Determine optionType from the shadow symbol
    shadow_symbol = deviation.get('shadow_symbol', '')
    if shadow_symbol.endswith('CE'):
      option_type = 'CE'
    elif shadow_symbol.endswith('PE'):
      option_type = 'PE'

    for trade in trademanager.trades:
      if trade.strategy != strategy_name:
        continue
      if trade.tradeState != TradeState.ACTIVE:
        continue
      if option_type:
        trade_option_type = trade.optionType or (
            'CE' if trade.tradingSymbol.endswith('CE') else
            'PE' if trade.tradingSymbol.endswith('PE') else None
        )
        if trade_option_type != option_type:
          continue
      if not trade.slOrder:
        continue
      # Only apply if shadow SL is actually looser than live SL (sanity check)
      # LONG: shadow SL is lower (further from price) = looser
      # SHORT: shadow SL is higher (further from price) = looser
      if trade.direction == Direction.LONG and shadow_sl >= trade.stopLoss:
        continue
      if trade.direction == Direction.SHORT and shadow_sl <= trade.stopLoss:
        continue
      omp = OrderModifyParams()
      omp.newTriggerPrice = Utils.roundToNSEPrice(shadow_sl)
      omp.newPrice = Utils.roundToNSEPrice(
          shadow_sl * (0.99 if trade.direction == Direction.LONG else 1.01))
      try:
        for sl_order in trade.slOrder:
          trademanager.getOrderManager(trademanager.name).modifyOrder(sl_order, omp, trade.qty)
        old_sl = trade.stopLoss
        trade.stopLoss = shadow_sl
        import logging
        logging.info('Shadow approval: Expanded SL from %.2f to %.2f for trade %s', old_sl, shadow_sl, trade.tradeID)
      except Exception as e:
        import logging
        logging.error('Shadow approval: Failed to modify SL for trade %s: %s', trade.tradeID, str(e))
      break

  # Remove this deviation
  sm.deviations = [d for d in sm.deviations if d.get('deviation_id') != deviation_id]

  # Unpause the strategy if no pausing deviations remain for it
  remaining_pausing = any(
      d for d in sm.deviations
      if d['strategy'] == strategy_name and d['type'] in (DeviationType.SL_HIT_IN_SHADOW, DeviationType.SL_TIGHTER)
  )
  if not remaining_pausing:
    trademanager.pausedStrategies.discard(strategy_name)

  return jsonify({'ok': True})


@token_required
def getQuote(trademanager, short_code):
  ms = ManualStrategy.getInstance(short_code=short_code)
  ul = request.args["index"]
  strike = request.args["strike"]
  iType = request.args["type"]

  from core.BaseStrategy import BaseStrategy
  symbolConfig = BaseStrategy.SYMBOL_CONFIG[ul]
  ms.exchange = symbolConfig["exchange"]

  if symbolConfig["expiryType"] == "weekly":
    tradingSymbol = Utils.prepareWeeklyOptionsSymbol(ul, strike, iType, expiryDay=symbolConfig["expiryDay"])
  else:
    tradingSymbol = Utils.prepareMonthlyOptionsSymbol(ul, strike, iType, expiryDay=symbolConfig["expiryDay"])
  quote = ms.getQuote(tradingSymbol)

  return str(quote.lastTradedPrice)