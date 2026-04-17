import logging
import threading
from datetime import datetime, timedelta

from trademgmt.BacktestManager import BacktestManager
from trademgmt.ShadowDeviation import DeviationType, DeviationStatus
from trademgmt.TradeState import TradeState
from utils.Utils import Utils


class ShadowManager(BacktestManager):
    """
    Replays today's candles from market open up to (now - 1 candle) using the same
    algo/strategy code as the live run, but without placing real orders.

    After the replay, compares shadow trades against live trades and populates
    self.deviations with any SL discrepancies.

    Intended use: create a fresh instance per shadow run, call startStrategies() on it,
    then call run() — same pattern as BacktestManager per-day runs.
    """

    def __init__(self, short_code, algo_instance, live_trade_manager):
        today_str = Utils.getTodayDateStr(Utils.getExchangeTimestamp(short_code))
        # Use a fixed dummy run_timestamp so shadow files are overwritten each run
        run_timestamp = 'shadow'
        # Use short_code + '_shadow' as thread name so strategy singletons (keyed by short_code)
        # don't overwrite the live strategy instances during the shadow replay.
        super().__init__(
            name=short_code + '_shadow',
            args=(live_trade_manager._accessToken, algo_instance, today_str, run_timestamp)
        )
        self.live_tm = live_trade_manager
        self.deviations = []
        self.completed_at = None
        self.is_running = False

    # ------------------------------------------------------------------
    # Overrides
    # ------------------------------------------------------------------

    def setupBacktestEnvironment(self):
        self.quotes.candles_table = self.live_tm.quotes.candles_table

    def run(self):
        """
        Replay today's candles up to (now minus the last complete 1-min candle),
        then compute deviations against live trades.
        """
        self.is_running = True
        logging.info('ShadowManager: Starting shadow replay for %s', self.test_date)

        nifty_map = self.timestamp_maps.get('NIFTY 50', {})
        if not nifty_map:
            logging.warning('ShadowManager: No NIFTY 50 data, aborting shadow run')
            self.is_running = False
            return

        # Cutoff: last closed 1-min candle (current minute is still forming)
        cutoff = datetime.now().replace(second=0, microsecond=0) - timedelta(minutes=1)

        nifty_candles = [
            nifty_map[ts] for ts in sorted(nifty_map.keys())
            if ts <= cutoff
        ]

        if not nifty_candles:
            logging.warning('ShadowManager: No candles before cutoff %s', cutoff)
            self.is_running = False
            return

        logging.info('ShadowManager: Replaying %d candles up to %s', len(nifty_candles), cutoff)

        for candle_index, nifty_candle in enumerate(nifty_candles):
            current_timestamp = nifty_candle['date']

            self.symbolToCMPMap['NIFTY 50'] = nifty_candle['close']
            self.symbolToCMPMap['exchange_timestamp'] = current_timestamp

            for symbol, ts_map in self.timestamp_maps.items():
                if current_timestamp in ts_map:
                    self.symbolToCMPMap[symbol] = ts_map[current_timestamp]['close']

            if candle_index == 0:
                self._processStrategies(current_timestamp)
                self.simulateTickerUpdates(nifty_candle, current_timestamp)
                continue

            self._refreshActiveTradePnl()
            self._processStrategies(current_timestamp)

            try:
                self.simulateTickerUpdates(nifty_candle, current_timestamp)
                self.checkStrategyHealth()
                self.fetchAndUpdateAllTradeOrders()
                self.trackAndUpdateAllTrades()
            except Exception:
                logging.exception('ShadowManager: Exception during candle replay at %s', current_timestamp)

        logging.info('ShadowManager: Replay complete — computing deviations')
        self._computeDeviations()
        self.completed_at = datetime.now()
        self.is_running = False
        logging.info('ShadowManager: Done. %d deviation(s) found.', len(self.deviations))

    # ------------------------------------------------------------------
    # Deviation detection
    # ------------------------------------------------------------------

    PAUSING_TYPES = {DeviationType.SL_HIT_IN_SHADOW, DeviationType.SL_TIGHTER}

    def _computeDeviations(self):
        self.deviations = []
        live_trades = self.live_tm.trades
        logging.info('ShadowManager: _computeDeviations: shadow trades=%d, live trades=%d', len(self.trades), len(live_trades))
        # Reset all shadow-driven pauses — this run is the source of truth
        self.live_tm.pausedStrategies.clear()

        matched_live_ids = set()   # id(live_trade) already paired
        matched_shadow_ids = set() # id(shadow_trade) already paired

        for s_trade in self.trades:
            if s_trade.tradeState == TradeState.DISABLED:
                continue

            live_match, is_exact = self._findLiveMatch(s_trade, live_trades, matched_live_ids)

            if live_match is None:
                # Only flag active shadow trades — shadow may run ahead of live on re-entries.
                if s_trade.tradeState == TradeState.ACTIVE:
                    self._append_deviation({
                        'type': DeviationType.MISSING_IN_LIVE,
                        'strategy': s_trade.strategy,
                        'shadow_symbol': s_trade.tradingSymbol,
                        'live_symbol': None,
                        'shadow_sl': s_trade.stopLoss,
                        'live_sl': None,
                        'shadow_state': s_trade.tradeState,
                        'live_state': None,
                        'shadow_exit_reason': s_trade.exitReason,
                    })
                continue

            matched_live_ids.add(id(live_match))
            matched_shadow_ids.add(id(s_trade))

            # Strike mismatch (same strategy/direction but different symbol) — flag only, no pause
            if not is_exact:
                already_handled = live_match.shadowDeviationStatus.get(DeviationType.STRIKE_MISMATCH) in (
                    DeviationStatus.HANDLED, DeviationStatus.IGNORED
                )
                if not already_handled:
                    self._append_deviation({
                        'type': DeviationType.STRIKE_MISMATCH,
                        'strategy': s_trade.strategy,
                        'shadow_symbol': s_trade.tradingSymbol,
                        'live_symbol': live_match.tradingSymbol,
                        'shadow_sl': s_trade.stopLoss,
                        'live_sl': live_match.stopLoss,
                        'shadow_state': s_trade.tradeState,
                        'live_state': live_match.tradeState,
                        'shadow_exit_reason': s_trade.exitReason,
                    })
                continue

            # SL_HIT_IN_SHADOW: shadow completed via SL but live trade still active
            shadow_sl_hit = (
                s_trade.tradeState == TradeState.COMPLETED
                and s_trade.exitReason in ('SL HIT', 'TRAIL SL HIT', 'STGY SL HIT', 'STGY TRAIL SL HIT', 'ALGO SL HIT', 'ALGO TRAIL SL HIT')
            )
            live_still_active = live_match.tradeState == TradeState.ACTIVE

            if shadow_sl_hit and live_still_active:
                self._append_deviation({
                    'type': DeviationType.SL_HIT_IN_SHADOW,
                    'strategy': s_trade.strategy,
                    'shadow_symbol': s_trade.tradingSymbol,
                    'live_symbol': live_match.tradingSymbol,
                    'shadow_sl': s_trade.stopLoss,
                    'live_sl': live_match.stopLoss,
                    'shadow_state': s_trade.tradeState,
                    'live_state': live_match.tradeState,
                    'shadow_exit_reason': s_trade.exitReason,
                })
                continue

            # SL_TIGHTER: live SL is tighter than shadow by 1% or more
            if (s_trade.tradeState == TradeState.ACTIVE
                    and live_match.tradeState == TradeState.ACTIVE
                    and s_trade.stopLoss and live_match.stopLoss):
                from models.Direction import Direction
                diff_pct = abs(live_match.stopLoss - s_trade.stopLoss) / s_trade.stopLoss * 100
                is_tighter = (
                    diff_pct >= 1.0 and (
                        (s_trade.direction == Direction.LONG and live_match.stopLoss > s_trade.stopLoss) or
                        (s_trade.direction == Direction.SHORT and live_match.stopLoss < s_trade.stopLoss)
                    )
                )
                if is_tighter:
                    self._append_deviation(self._sl_tighter_deviation(s_trade, live_match))

        # Extra in live: live has active trades that shadow has no match for — flag only
        for l_trade in live_trades:
            if l_trade.tradeState != TradeState.ACTIVE:
                continue
            if id(l_trade) in matched_live_ids:
                continue
            live_strategy_instance = self.live_tm.strategyToInstanceMap.get(l_trade.strategy)
            if live_strategy_instance and live_strategy_instance.__class__.skip_in_shadow:
                continue
            if l_trade.shadowDeviationStatus.get(DeviationType.EXTRA_IN_LIVE) in (
                DeviationStatus.HANDLED, DeviationStatus.IGNORED
            ):
                continue
            shadow_match = self._findShadowMatch(l_trade, self.trades, matched_shadow_ids)
            if shadow_match is None:
                self._append_deviation({
                    'type': DeviationType.EXTRA_IN_LIVE,
                    'strategy': l_trade.strategy,
                    'shadow_symbol': None,
                    'live_symbol': l_trade.tradingSymbol,
                    'live_trade_id': l_trade.tradeID,
                    'shadow_sl': None,
                    'live_sl': l_trade.stopLoss,
                    'shadow_state': None,
                    'live_state': l_trade.tradeState,
                    'shadow_exit_reason': None,
                })

    @staticmethod
    def _resolveOptionType(trade):
        """Return the option type for a trade, falling back to symbol suffix if optionType is unset."""
        if trade.optionType:
            return trade.optionType
        sym = trade.tradingSymbol or ''
        if sym.endswith('CE'):
            return 'CE'
        if sym.endswith('PE'):
            return 'PE'
        return None

    def _findLiveMatch(self, shadow_trade, live_trades, already_matched):
        """
        Match by strategy + direction + leg identity + tradingSymbol (exact).

        Leg identity priority:
          1. tag (strategy-assigned, e.g. CE_ATM) — used when both trades have one
          2. optionType resolved from symbol suffix (CE/PE) — fallback

        Falls back to fuzzy match (same leg identity, different symbol) when no exact
        symbol match is found. Skips trades already matched to another shadow trade.
        Returns (live_trade, is_exact_match).
        """
        shadow_tag = shadow_trade.tag
        shadow_option_type = self._resolveOptionType(shadow_trade)
        fuzzy = None
        for lt in live_trades:
            if id(lt) in already_matched:
                continue
            if lt.tradeState == TradeState.DISABLED:
                continue
            if lt.strategy != shadow_trade.strategy:
                continue
            if lt.direction != shadow_trade.direction:
                continue
            # Leg identity check
            if shadow_tag and lt.tag:
                if lt.tag != shadow_tag:
                    continue
            elif shadow_trade.isOptions and lt.isOptions:
                if self._resolveOptionType(lt) != shadow_option_type:
                    continue
            if lt.tradingSymbol == shadow_trade.tradingSymbol:
                return lt, True
            if fuzzy is None:
                fuzzy = lt
        if fuzzy is not None:
            return fuzzy, False
        return None, False

    def _findShadowMatch(self, live_trade, shadow_trades, already_matched):
        live_tag = live_trade.tag
        live_option_type = self._resolveOptionType(live_trade)
        for st in shadow_trades:
            if id(st) in already_matched:
                continue
            if st.tradeState == TradeState.DISABLED:
                continue
            if st.strategy != live_trade.strategy:
                continue
            if st.direction != live_trade.direction:
                continue
            # Leg identity check
            if live_tag and st.tag:
                if st.tag != live_tag:
                    continue
            elif live_trade.isOptions and st.isOptions:
                if self._resolveOptionType(st) != live_option_type:
                    continue
            return st
        return None

    def _sl_tighter_deviation(self, s_trade, live_match):
        return {
            'type': DeviationType.SL_TIGHTER,
            'strategy': s_trade.strategy,
            'shadow_symbol': s_trade.tradingSymbol,
            'live_symbol': live_match.tradingSymbol,
            'shadow_sl': s_trade.stopLoss,
            'live_sl': live_match.stopLoss,
            'shadow_state': s_trade.tradeState,
            'live_state': live_match.tradeState,
            'shadow_exit_reason': s_trade.exitReason,
        }

    def _append_deviation(self, deviation):
        """Append deviation, assign a unique id, and pause the strategy if it's a pausing type."""
        import uuid
        deviation['deviation_id'] = str(uuid.uuid4())
        deviation['detected_at'] = datetime.now().strftime('%H:%M:%S')
        self.deviations.append(deviation)
        if deviation['type'] in self.PAUSING_TYPES:
            strategy_name = deviation['strategy']
            self.live_tm.pausedStrategies.add(strategy_name)
            logging.warning('ShadowManager: Pausing strategy %s due to %s deviation', strategy_name, deviation['type'])

