/**
 * Shared backtest results rendering functions.
 * Used by both backtesting.html and backtest_history.html.
 *
 * All statistics are computed client-side by computeResults().
 * The server sends only raw trade data (CSV) + minimal run metadata.
 *
 * Rendering is done via Knockout.js ViewModels (makeBacktestVM).
 */

/** ISO week key from a YYYY-MM-DD string, e.g. "2026-W15". */
function isoWeekKey(dateStr) {
  const d = new Date(dateStr + 'T00:00:00');
  const thu = new Date(d);
  thu.setUTCDate(thu.getUTCDate() + 3 - ((thu.getUTCDay() + 6) % 7));
  const isoYear = thu.getUTCFullYear();
  const jan4 = new Date(Date.UTC(isoYear, 0, 4));
  const weekNum = 1 + Math.round(((thu - jan4) / 86400000 - 3 + ((jan4.getUTCDay() + 6) % 7)) / 7);
  return isoYear + '-W' + String(weekNum).padStart(2, '0');
}

/** Compute weekly win stats from parallel arrays of dates and PnLs. */
function weeklyWinStats(dates, pnls) {
  const byWeek = {};
  for (let i = 0; i < dates.length; i++) {
    const wk = isoWeekKey(dates[i]);
    byWeek[wk] = (byWeek[wk] || 0) + (pnls[i] || 0);
  }
  const total = Object.keys(byWeek).length;
  const wins = Object.values(byWeek).filter(function(v) { return v > 0; }).length;
  return { byWeek: byWeek, total_weeks: total, winning_weeks: wins, weekly_success_rate: total > 0 ? (wins / total * 100) : 0 };
}

/** Format a number with Indian lakh-style commas, no decimal places. */
function fmtPnl(n) {
  const intPart = String(Math.round(Math.abs(n)));
  // Indian grouping: last 3 digits, then groups of 2
  const lastThree = intPart.slice(-3);
  const rest = intPart.slice(0, -3);
  const grouped = rest ? rest.replace(/\B(?=(\d{2})+(?!\d))/g, ',') + ',' + lastThree : lastThree;
  return (n < 0 ? '-' : '') + grouped;
}

/**
 * Compute full results object from a runs-index entry and an array of trade objects.
 *
 * @param {object} runEntry  - {run_id, algo, start_date, end_date, comment,
 *                              days:[{date, pnl, strategies:{name:{days_to_expiry, highest_pnl, lowest_pnl}}}]}
 * @param {Array}  trades    - trade objects from the CSV API
 * @returns {object} results - same shape expected by the KO ViewModel
 */
function computeResults(runEntry, trades) {
  const days = runEntry.days || [];

  // Build daily_breakdown from trades + day metadata
  // daily_breakdown[date][strategy] = {pnl, trades, days_to_expiry, highest_pnl, lowest_pnl}
  const daily_breakdown = {};
  for (const day of days) {
    daily_breakdown[day.date] = {
      _run_id: runEntry.run_id,
      _highest_pnl: day.highest_pnl || 0,
      _lowest_pnl: day.lowest_pnl || 0,
    };
    for (const [strat, meta] of Object.entries(day.strategies || {})) {
      daily_breakdown[day.date][strat] = {
        pnl: 0,
        trades: 0,
        days_to_expiry: meta.days_to_expiry != null ? meta.days_to_expiry : null,
        highest_pnl: meta.highest_pnl || 0,
        lowest_pnl: meta.lowest_pnl || 0,
        qty: 0,
      };
    }
  }

  // Accumulate trade PnL into daily_breakdown
  for (const t of trades) {
    const date = t.date;
    const strat = t.strategy;
    const pnl = t.pnl || 0;
    if (!daily_breakdown[date]) {
      daily_breakdown[date] = { _run_id: runEntry.run_id };
    }
    if (!daily_breakdown[date][strat]) {
      daily_breakdown[date][strat] = { pnl: 0, trades: 0, days_to_expiry: null, highest_pnl: 0, lowest_pnl: 0, qty: 0 };
    }
    daily_breakdown[date][strat].pnl += pnl;
    daily_breakdown[date][strat].trades += 1;
    daily_breakdown[date][strat].qty = Math.max(daily_breakdown[date][strat].qty, t.qty || 0);
  }

  // Collect all strategy names
  const strategySet = new Set();
  for (const dayData of Object.values(daily_breakdown)) {
    for (const k of Object.keys(dayData)) {
      if (!k.startsWith('_')) strategySet.add(k);
    }
  }
  const allStrategies = [...strategySet].sort();

  // Per-strategy aggregation across days
  const strategies = {};
  for (const strat of allStrategies) {
    let total_pnl = 0, total_trades = 0, total_days = 0, profitable_days = 0;
    let total_winning_pnl = 0, total_losing_pnl = 0;

    for (const [date, dayData] of Object.entries(daily_breakdown)) {
      if (!dayData[strat]) continue;
      total_days += 1;
      const dpnl = dayData[strat].pnl;
      total_pnl += dpnl;
      total_trades += dayData[strat].trades;
      if (dpnl > 0) { profitable_days += 1; total_winning_pnl += dpnl; }
      else if (dpnl < 0) { total_losing_pnl += Math.abs(dpnl); }
    }

    const losing_days = total_days - profitable_days;
    const avg_win = profitable_days > 0 ? total_winning_pnl / profitable_days : 0;
    const avg_loss = losing_days > 0 ? total_losing_pnl / losing_days : 0;
    let kelly_ratio = 0;
    if (profitable_days > 0 && losing_days > 0 && avg_loss > 0) {
      const W = profitable_days / total_days;
      kelly_ratio = W - ((1 - W) / (avg_win / avg_loss));
    }

    strategies[strat] = {
      total_pnl,
      total_trades,
      total_days,
      profitable_days,
      avg_pnl_per_day: total_days > 0 ? total_pnl / total_days : 0,
      win_rate: total_days > 0 ? (profitable_days / total_days * 100) : 0,
      avg_win,
      avg_loss,
      kelly_ratio,
    };
  }

  // Overall daily PnL series (sorted by date)
  const sortedDates = Object.keys(daily_breakdown).sort();
  const dailyPnlSeries = sortedDates.map(date => {
    return Object.entries(daily_breakdown[date])
      .filter(([k]) => !k.startsWith('_'))
      .reduce((sum, [, v]) => sum + v.pnl, 0);
  });

  const days_tested = dailyPnlSeries.length;
  const total_pnl = dailyPnlSeries.reduce((s, v) => s + v, 0);
  const total_trades = trades.length;

  let profitable_days = 0, total_winning_daily = 0, total_losing_daily = 0;
  let best_day = null, best_day_pnl = null, worst_day = null, worst_day_pnl = null;

  for (let i = 0; i < sortedDates.length; i++) {
    const date = sortedDates[i];
    const dpnl = dailyPnlSeries[i];
    if (best_day_pnl === null || dpnl > best_day_pnl) { best_day_pnl = dpnl; best_day = date; }
    if (worst_day_pnl === null || dpnl < worst_day_pnl) { worst_day_pnl = dpnl; worst_day = date; }
    if (dpnl > 0) { profitable_days += 1; total_winning_daily += dpnl; }
    else if (dpnl < 0) { total_losing_daily += Math.abs(dpnl); }
  }

  const losing_days_total = days_tested - profitable_days;
  const avg_daily_win = profitable_days > 0 ? total_winning_daily / profitable_days : 0;
  const avg_daily_loss = losing_days_total > 0 ? total_losing_daily / losing_days_total : 0;

  let kelly_ratio = 0;
  if (profitable_days > 0 && losing_days_total > 0 && avg_daily_loss > 0) {
    const W = profitable_days / days_tested;
    kelly_ratio = W - ((1 - W) / (avg_daily_win / avg_daily_loss));
  }

  // Max drawdown
  let max_drawdown = 0, peak = 0, cumulative = 0;
  for (const pnl of dailyPnlSeries) {
    cumulative += pnl;
    if (cumulative > peak) peak = cumulative;
    const dd = peak - cumulative;
    if (dd > max_drawdown) max_drawdown = dd;
  }

  // Sharpe ratio (annualised)
  let sharpe_ratio = 0;
  if (dailyPnlSeries.length > 1) {
    const mean = total_pnl / days_tested;
    const variance = dailyPnlSeries.reduce((s, v) => s + (v - mean) ** 2, 0) / (days_tested - 1);
    const stdev = Math.sqrt(variance);
    if (stdev > 0) sharpe_ratio = (mean / stdev) * Math.sqrt(252);
  }

  // Profit factor
  const profit_factor = total_losing_daily > 0 ? total_winning_daily / total_losing_daily : null;

  // Streaks
  let max_win_streak = 0, max_loss_streak = 0, cur_win = 0, cur_loss = 0;
  for (const pnl of dailyPnlSeries) {
    if (pnl > 0) { cur_win += 1; cur_loss = 0; }
    else if (pnl < 0) { cur_loss += 1; cur_win = 0; }
    else { cur_win = 0; cur_loss = 0; }
    if (cur_win > max_win_streak) max_win_streak = cur_win;
    if (cur_loss > max_loss_streak) max_loss_streak = cur_loss;
  }

  // Weekly success rate
  const { total_weeks, winning_weeks, weekly_success_rate } = weeklyWinStats(sortedDates, dailyPnlSeries);

  // DTE breakdown
  const dte_breakdown = {};
  for (const [date, dayData] of Object.entries(daily_breakdown)) {
    const date_dte_totals = {};
    for (const [strat, sdata] of Object.entries(dayData)) {
      if (strat.startsWith('_')) continue;
      const dte = sdata.days_to_expiry;
      if (dte == null) continue;

      if (!dte_breakdown[strat]) dte_breakdown[strat] = {};
      if (!dte_breakdown[strat][dte]) dte_breakdown[strat][dte] = { pnl: 0, trades: 0, days: 0, wins: 0, winning_pnl: 0, losing_pnl: 0 };
      dte_breakdown[strat][dte].pnl += sdata.pnl;
      dte_breakdown[strat][dte].trades += sdata.trades;
      dte_breakdown[strat][dte].days += 1;
      if (sdata.pnl > 0) { dte_breakdown[strat][dte].wins += 1; dte_breakdown[strat][dte].winning_pnl += sdata.pnl; }
      else if (sdata.pnl < 0) { dte_breakdown[strat][dte].losing_pnl += Math.abs(sdata.pnl); }

      if (!date_dte_totals[dte]) date_dte_totals[dte] = { pnl: 0, trades: 0 };
      date_dte_totals[dte].pnl += sdata.pnl;
      date_dte_totals[dte].trades += sdata.trades;
    }

    if (!dte_breakdown['_TOTAL']) dte_breakdown['_TOTAL'] = {};
    for (const [dte, totals] of Object.entries(date_dte_totals)) {
      if (!dte_breakdown['_TOTAL'][dte]) dte_breakdown['_TOTAL'][dte] = { pnl: 0, trades: 0, days: 0, wins: 0, winning_pnl: 0, losing_pnl: 0 };
      dte_breakdown['_TOTAL'][dte].pnl += totals.pnl;
      dte_breakdown['_TOTAL'][dte].trades += totals.trades;
      dte_breakdown['_TOTAL'][dte].days += 1;
      const dayTotal = Object.entries(daily_breakdown[date])
        .filter(([k]) => !k.startsWith('_'))
        .reduce((s, [, v]) => s + v.pnl, 0);
      if (dayTotal > 0) { dte_breakdown['_TOTAL'][dte].wins += 1; dte_breakdown['_TOTAL'][dte].winning_pnl += dayTotal; }
      else if (dayTotal < 0) { dte_breakdown['_TOTAL'][dte].losing_pnl += Math.abs(dayTotal); }
    }
  }

  return {
    run_id: runEntry.run_id,
    algo: runEntry.algo,
    comment: runEntry.comment,
    start_date: runEntry.start_date,
    end_date: runEntry.end_date,
    strategies,
    daily_breakdown,
    total_pnl,
    total_trades,
    days_tested,
    profitable_days,
    win_rate: days_tested > 0 ? (profitable_days / days_tested * 100) : 0,
    avg_daily_pnl: days_tested > 0 ? total_pnl / days_tested : 0,
    avg_daily_win,
    avg_daily_loss,
    kelly_ratio,
    max_drawdown,
    sharpe_ratio,
    profit_factor,
    max_win_streak,
    max_loss_streak,
    best_day,
    best_day_pnl,
    worst_day,
    worst_day_pnl,
    dte_breakdown,
    total_weeks,
    winning_weeks,
    weekly_success_rate,
  };
}

/**
 * Fetch trades for a run, compute results, and populate a ViewModel.
 * @param {string} shortCode
 * @param {object} runEntry  - minimal run metadata from the index
 * @param {object} vm        - a ViewModel created by makeBacktestVM
 * @param {Function} [onDone]  - optional callback(results) after load
 */
function fetchAndRenderResults(shortCode, runEntry, vm, onDone) {
  $.ajax({
    url: '/backtesting/' + shortCode + '/trades',
    type: 'GET',
    data: { run_id: runEntry.run_id },
    success: function(response) {
      if (!response.success) {
        vm.statusMessage('<div class="alert alert-warning">No trades found for this run.</div>');
        if (onDone) onDone(null);
        return;
      }
      const results = computeResults(runEntry, response.trades || []);
      vm.results(results);
      vm.statusMessage('');
      if (onDone) onDone(results);
    },
    error: function(xhr) {
      const msg = xhr.responseJSON ? xhr.responseJSON.error : 'Failed to load trades';
      vm.statusMessage('<div class="alert alert-danger"><strong>Error:</strong> ' + msg + '</div>');
      if (onDone) onDone(null);
    },
  });
}

/**
 * Derive overall metrics from a filtered daily PnL series.
 * Shared helper used by the filteredMetrics computed.
 * @param {number[]} dailyPnlSeries  - array of daily net PnL values
 * @param {number}   totalTrades     - trade count (informational only)
 * @returns {object}
 */
function deriveMetrics(dailyPnlSeries, totalTrades, sortedDates) {
  const days_tested = dailyPnlSeries.length;
  const total_pnl = dailyPnlSeries.reduce((s, v) => s + v, 0);

  let profitable_days = 0, total_winning = 0, total_losing = 0;
  let best_day_pnl = null, worst_day_pnl = null;

  for (const pnl of dailyPnlSeries) {
    if (best_day_pnl === null || pnl > best_day_pnl) best_day_pnl = pnl;
    if (worst_day_pnl === null || pnl < worst_day_pnl) worst_day_pnl = pnl;
    if (pnl > 0) { profitable_days++; total_winning += pnl; }
    else if (pnl < 0) { total_losing += Math.abs(pnl); }
  }

  const losing_days = days_tested - profitable_days;
  const avg_daily_win = profitable_days > 0 ? total_winning / profitable_days : 0;
  const avg_daily_loss = losing_days > 0 ? total_losing / losing_days : 0;

  let kelly_ratio = 0;
  if (profitable_days > 0 && losing_days > 0 && avg_daily_loss > 0) {
    const W = profitable_days / days_tested;
    kelly_ratio = W - ((1 - W) / (avg_daily_win / avg_daily_loss));
  }

  let max_drawdown = 0, peak = 0, cumulative = 0;
  for (const pnl of dailyPnlSeries) {
    cumulative += pnl;
    if (cumulative > peak) peak = cumulative;
    const dd = peak - cumulative;
    if (dd > max_drawdown) max_drawdown = dd;
  }

  let sharpe_ratio = 0;
  if (days_tested > 1) {
    const mean = total_pnl / days_tested;
    const variance = dailyPnlSeries.reduce((s, v) => s + (v - mean) ** 2, 0) / (days_tested - 1);
    const stdev = Math.sqrt(variance);
    if (stdev > 0) sharpe_ratio = (mean / stdev) * Math.sqrt(252);
  }

  const profit_factor = total_losing > 0 ? total_winning / total_losing : null;

  let max_win_streak = 0, max_loss_streak = 0, cur_win = 0, cur_loss = 0;
  for (const pnl of dailyPnlSeries) {
    if (pnl > 0) { cur_win++; cur_loss = 0; }
    else if (pnl < 0) { cur_loss++; cur_win = 0; }
    else { cur_win = 0; cur_loss = 0; }
    if (cur_win > max_win_streak) max_win_streak = cur_win;
    if (cur_loss > max_loss_streak) max_loss_streak = cur_loss;
  }

  const sortedSeries = dailyPnlSeries.slice().sort(function(a, b) { return b - a; });

  // Weekly success rate
  const wkStats = sortedDates && sortedDates.length === dailyPnlSeries.length
    ? weeklyWinStats(sortedDates, dailyPnlSeries)
    : { total_weeks: 0, winning_weeks: 0, weekly_success_rate: 0 };

  return {
    total_trades: totalTrades,
    total_pnl,
    days_tested,
    profitable_days,
    win_rate: days_tested > 0 ? profitable_days / days_tested * 100 : 0,
    avg_daily_pnl: days_tested > 0 ? total_pnl / days_tested : 0,
    avg_daily_win,
    avg_daily_loss,
    kelly_ratio,
    max_drawdown,
    sharpe_ratio,
    profit_factor,
    max_win_streak,
    max_loss_streak,
    best_day_pnl,
    worst_day_pnl,
    total_weeks: wkStats.total_weeks,
    winning_weeks: wkStats.winning_weeks,
    weekly_success_rate: wkStats.weekly_success_rate,
    max1_pnl: sortedSeries.length > 0 ? sortedSeries[0] : null,
    max2_pnl: sortedSeries.length > 1 ? sortedSeries[1] : null,
    min1_pnl: sortedSeries.length > 0 ? sortedSeries[sortedSeries.length - 1] : null,
    min2_pnl: sortedSeries.length > 1 ? sortedSeries[sortedSeries.length - 2] : null,
  };
}

// ── Plotly chart helper ──────────────────────────────────────────────────────

/**
 * Render or update a Plotly cumulative-PnL chart inside `el`.
 * @param {HTMLElement} el
 * @param {{ dates: string[], traces: {name: string, y: number[]}[] }} data
 */
function renderEquityCurve(el, data) {
  if (!data || !data.dates || !data.dates.length) {
    Plotly.purge(el);
    return;
  }

  const COLORS = [
    '#2196F3', '#FF9800', '#4CAF50', '#E91E63', '#9C27B0',
    '#00BCD4', '#FF5722', '#795548', '#607D8B', '#F44336',
  ];

  const traces = data.traces.map(function(t, i) {
    const isTotal = t.name === 'Total';
    const isDrawdown = t.isDrawdown;
    if (isDrawdown) {
      return {
        x: data.dates,
        y: t.y,
        name: t.name,
        type: 'scatter',
        mode: 'lines',
        fill: 'tozeroy',
        fillcolor: 'rgba(220,0,0,0.15)',
        line: { color: 'rgba(180,0,0,0.5)', width: 1 },
        yaxis: 'y2',
        hovertemplate: '%{y:,.0f}<extra>Drawdown</extra>',
      };
    }
    return {
      x: data.dates,
      y: t.y,
      name: t.name,
      type: 'scatter',
      mode: 'lines',
      line: {
        width: isTotal ? 3 : 1.5,
        dash: isTotal ? 'dot' : 'solid',
        color: isTotal ? '#000' : COLORS[i % COLORS.length],
      },
    };
  });

  const layout = {
    margin: { t: 20, r: 70, b: 60, l: 70 },
    height: 320,
    xaxis: { type: 'date', tickformat: '%b %y' },
    yaxis: { title: 'Cumulative PnL', tickformat: ',.0f' },
    yaxis2: { title: 'Drawdown', tickformat: ',.0f', overlaying: 'y', side: 'right', showgrid: false },
    legend: { orientation: 'h', y: -0.2 },
    hovermode: 'x unified',
    plot_bgcolor: '#fff',
    paper_bgcolor: '#fff',
  };

  Plotly.react(el, traces, layout, { responsive: true, displayModeBar: false });
}

// ── KO custom bindings ───────────────────────────────────────────────────────

ko.bindingHandlers.fmtPnlText = {
  update: function(el, va) {
    var v = parseFloat(ko.unwrap(va()));
    if (isNaN(v)) { el.textContent = '-'; return; }
    el.textContent = fmtPnl(v);
  }
};

// Renders a Plotly equity curve; re-renders whenever the bound value changes.
ko.bindingHandlers.equityCurve = {
  update: function(el, va) {
    renderEquityCurve(el, ko.unwrap(va()));
  }
};

// Sets both textContent and a text-success/text-danger class
ko.bindingHandlers.pnlCell = {
  update: function(el, va) {
    var v = parseFloat(ko.unwrap(va()));
    if (isNaN(v)) { el.textContent = '-'; return; }
    el.textContent = fmtPnl(v);
    el.classList.remove('text-success', 'text-danger', 'text-muted');
    el.classList.add(v > 0 ? 'text-success' : v < 0 ? 'text-danger' : 'text-muted');
  }
};

// Manages a tri-state checkbox: checked, unchecked, or indeterminate.
// Expects the bound value to be an object: { checked: ko.observable(bool), indeterminate: ko.observable(bool), toggle: fn }
ko.bindingHandlers.triStateCheckbox = {
  init: function(el, va) {
    el.addEventListener('click', function(e) {
      e.preventDefault();
      ko.unwrap(va()).toggle();
    });
  },
  update: function(el, va) {
    var v = ko.unwrap(va());
    var chk = ko.unwrap(v.checked);
    var ind = ko.unwrap(v.indeterminate);
    el.indeterminate = ind;
    el.checked = !ind && chk;
  }
};

// ── ViewModel factory ────────────────────────────────────────────────────────

const WEEKDAYS = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
const WEEKDAY_ORDER = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri'];

/**
 * Create a Knockout ViewModel for backtest results.
 * @param {string} shortCode  - algo short code for API calls
 * @returns {object} vm
 */
function makeBacktestVM(shortCode) {
  const vm = {
    shortCode: shortCode,
    results: ko.observable(null),
    statusMessage: ko.observable(''),

    // Collapse state
    dailyVisible: ko.observable(false),
    dteVisible: ko.observable(true),
    dteStratVisible: ko.observable(false),
    dteWiseStratVisible: ko.observable(false),
    weekdayVisible: ko.observable(false),
    weeklyVisible: ko.observable(false),
    monthlyVisible: ko.observable(true),
    chartVisible: ko.observable(true),

    // Sort state for daily breakdown
    sortCol: ko.observable('date'),
    sortDir: ko.observable(1),

    // Trade details modal
    modalTitle: ko.observable('Trade Details'),
    modalTrades: ko.observableArray([]),
    modalSummary: ko.observable(null),
    modalLoading: ko.observable(false),
  };

  vm.toggleDaily = function() { vm.dailyVisible(!vm.dailyVisible()); };
  vm.toggleDte   = function() { vm.dteVisible(!vm.dteVisible()); };

  vm.sortBy = function(col) {
    if (vm.sortCol() === col) {
      vm.sortDir(vm.sortDir() * -1);
    } else {
      vm.sortCol(col);
      vm.sortDir(1);
    }
  };

  vm.sortArrow = function(col) {
    return vm.sortCol() === col ? (vm.sortDir() === 1 ? ' ▲' : ' ▼') : '';
  };

  vm.isSortedBy = function(col) {
    return vm.sortCol() === col;
  };

  // ── Single source of truth: _dateStratChecked ────────────────────────────────
  // Keyed by "date|||strategy" → ko.observable(bool).
  // All filter logic reads from this map. All aggregator checkboxes write to it.
  vm._dateStratChecked = ko.observable({});

  // Pre-populate the map when results load, so all keys exist upfront.
  vm.results.subscribe(function(res) {
    if (!res || !res.daily_breakdown) { vm._dateStratChecked({}); return; }
    const map = {};
    for (const [date, dayData] of Object.entries(res.daily_breakdown)) {
      for (const k of Object.keys(dayData)) {
        if (!k.startsWith('_')) map[date + '|||' + k] = true;
      }
    }
    vm._dateStratChecked(map);
  });

  // Set all (date, strat) pairs matching a predicate to a given value.
  function setChecked(predicate, value) {
    const map = vm._dateStratChecked();
    for (const key of Object.keys(map)) {
      if (predicate(key)) map[key] = value;
    }
    // Notify KO that the map changed so computeds that read _dateStratChecked() re-run.
    vm._dateStratChecked.valueHasMutated();
  }

  // ── excludeTop2 state (per DTE) ──────────────────────────────────────────────
  // Declared before _effectiveMap so the computed can reference them.
  const _dteExcludeTop2 = {};
  const _excludeTop2Version = ko.observable(0);
  function getDteExcludeTop2(dte) {
    if (!_dteExcludeTop2[dte]) {
      _dteExcludeTop2[dte] = ko.observable(false);
      _dteExcludeTop2[dte].subscribe(function() { _excludeTop2Version(_excludeTop2Version() + 1); });
    }
    return _dteExcludeTop2[dte];
  }

  // ── Effective map: _dateStratChecked + excludeTop2 overrides ─────────────────
  // All aggregator computeds read this instead of _dateStratChecked directly.
  // When excludeTop2 is on for a DTE, the top 2 profit dates for each strategy
  // in that DTE are marked false in the effective map.
  vm._effectiveMap = ko.computed(function() {
    const base = vm._dateStratChecked();
    const res = vm.results();
    _excludeTop2Version();  // subscribe so any excludeTop2 toggle re-runs this computed
    if (!res || !res.daily_breakdown) return base;

    // Collect which DTEs have excludeTop2 on
    const activeDtes = [];
    for (const dte of Object.keys(_dteExcludeTop2)) {
      if (_dteExcludeTop2[dte]()) activeDtes.push(Number(dte));
    }

    if (activeDtes.length === 0) return base;

    // For each active DTE, find the top 2 profitable dates per strategy and mark them false
    const override = Object.assign({}, base);
    for (const dte of activeDtes) {
      // Group all checked (date, strat, pnl) entries by strategy for this DTE
      const byStrat = {};
      for (const [date, dayData] of Object.entries(res.daily_breakdown)) {
        for (const [strat, sd] of Object.entries(dayData)) {
          if (strat.startsWith('_')) continue;
          if (sd.days_to_expiry !== dte || sd.trades === 0) continue;
          const key = date + '|||' + strat;
          if (base[key] === false) continue;  // already excluded
          if (!byStrat[strat]) byStrat[strat] = [];
          byStrat[strat].push({ key, pnl: sd.pnl });
        }
      }
      // For each strategy, mark its top 2 profitable days as false
      for (const entries of Object.values(byStrat)) {
        entries.sort(function(a, b) { return b.pnl - a.pnl; });
        let skipped = 0;
        for (const e of entries) {
          if (skipped >= 2 || e.pnl <= 0) break;
          override[e.key] = false;
          skipped++;
        }
      }
    }
    return override;
  });

  // ── Derived: all strategy names ──────────────────────────────────────────────
  vm.allStrategies = ko.computed(function() {
    const res = vm.results();
    if (!res || !res.strategies) return [];
    return Object.keys(res.strategies).sort();
  });

  // ── Helper: kelly ratio from win/loss stats ──────────────────────────────────
  function calcKelly(wins, total_days, winning_pnl, losing_pnl) {
    const losing_days = total_days - wins;
    const avg_win = wins > 0 ? winning_pnl / wins : 0;
    const avg_loss = losing_days > 0 ? losing_pnl / losing_days : 0;
    let kelly_ratio = 0;
    if (wins > 0 && losing_days > 0 && avg_loss > 0) {
      const W = wins / total_days;
      kelly_ratio = W - ((1 - W) / (avg_win / avg_loss));
    }
    return { avg_win, avg_loss, kelly_ratio };
  }

  function calcDteRowMetrics(days, wins, winning_pnl, losing_pnl, trades, pnl, dayPnls) {
    const { avg_win, avg_loss, kelly_ratio } = calcKelly(wins, days, winning_pnl, losing_pnl);
    const sorted = (dayPnls || []).slice().sort(function(a, b) { return b - a; });
    return {
      days, wins, trades, total_pnl: pnl,
      avg_pnl: days > 0 ? pnl / days : 0,
      win_rate: days > 0 ? (wins / days * 100) : 0,
      avg_win, avg_loss, kelly_ratio,
      max1_pnl: sorted.length > 0 ? sorted[0] : null,
      max2_pnl: sorted.length > 1 ? sorted[1] : null,
      min1_pnl: sorted.length > 0 ? sorted[sorted.length - 1] : null,
      min2_pnl: sorted.length > 1 ? sorted[sorted.length - 2] : null,
    };
  }

  // ── Strategy-wise Performance table ──────────────────────────────────────────
  // Each row is an aggregator: its checkbox state reflects all (date, strat) pairs
  // for that strategy. Clicking drives the per-strategy DTE checkboxes.
  vm.strategyRows = ko.computed(function() {
    const res = vm.results();
    if (!res || !res.daily_breakdown) return [];
    const map = vm._effectiveMap();  // tracked

    return vm.allStrategies().map(function(name) {
      // Aggregate stats over all included (date, name) pairs
      let total_pnl = 0, total_trades = 0, total_days = 0, profitable_days = 0;
      let winning_pnl = 0, losing_pnl = 0;
      let checkedCount = 0, totalKeys = 0, allTrades = 0, uncheckedTrades = 0;
      const dayPnls = [];

      for (const [date, dayData] of Object.entries(res.daily_breakdown)) {
        const sd = dayData[name];
        if (!sd) continue;
        totalKeys++;
        allTrades += sd.trades;
        const key = date + '|||' + name;
        const included = map[key] !== false;
        if (included) {
          checkedCount++;
          if (sd.trades > 0) {
            total_pnl += sd.pnl;
            total_trades += sd.trades;
            total_days += 1;
            dayPnls.push(sd.pnl);
            if (sd.pnl > 0) { profitable_days++; winning_pnl += sd.pnl; }
            else if (sd.pnl < 0) { losing_pnl += Math.abs(sd.pnl); }
          }
        } else {
          uncheckedTrades += sd.trades;
        }
      }

      // Don't render rows that have no trades at all in daily_breakdown
      if (allTrades === 0) return null;

      const { avg_win, avg_loss, kelly_ratio } = calcKelly(profitable_days, total_days, winning_pnl, losing_pnl);
      const sortedDayPnls = dayPnls.slice().sort(function(a, b) { return b - a; });

      // Aggregator checkbox state — indeterminate only if excluded rows actually have trades
      const allChecked = checkedCount === totalKeys && totalKeys > 0;
      const someChecked = checkedCount > 0 && checkedCount < totalKeys && uncheckedTrades > 0;

      function toggleStrat() {
        const newVal = !allChecked;
        setChecked(function(key) { return key.endsWith('|||' + name); }, newVal);
      }

      return {
        name: name,
        total_pnl: total_pnl,
        avg_pnl_per_day: total_days > 0 ? total_pnl / total_days : 0,
        total_trades: total_trades,
        profitable_days: profitable_days,
        total_days: total_days,
        win_rate: total_days > 0 ? (profitable_days / total_days * 100) : 0,
        avg_win: avg_win,
        avg_loss: avg_loss,
        kelly_ratio: kelly_ratio,
        max1_pnl: sortedDayPnls.length > 0 ? sortedDayPnls[0] : null,
        max2_pnl: sortedDayPnls.length > 1 ? sortedDayPnls[1] : null,
        min1_pnl: sortedDayPnls.length > 0 ? sortedDayPnls[sortedDayPnls.length - 1] : null,
        min2_pnl: sortedDayPnls.length > 1 ? sortedDayPnls[sortedDayPnls.length - 2] : null,
        stratCheckbox: { checked: allChecked, indeterminate: someChecked, toggle: toggleStrat },
        isIncluded: allChecked || someChecked,
      };
    }).filter(Boolean);
  });

  // ── filteredMetrics: derive overall stats from _dateStratChecked ─────────────
  vm.filteredMetrics = ko.computed(function() {
    const res = vm.results();
    if (!res || !res.daily_breakdown) return null;
    const map = vm._effectiveMap();  // tracked

    const sortedDates = Object.keys(res.daily_breakdown).sort();
    let filteredTrades = 0;
    const dailySeries = [];
    const activeDates = [];

    for (const date of sortedDates) {
      const dayData = res.daily_breakdown[date];
      let dayPnl = 0;
      let dayActive = false;
      for (const [strat, sdata] of Object.entries(dayData)) {
        if (strat.startsWith('_')) continue;
        if (map[date + '|||' + strat] === false) continue;
        if (sdata.trades === 0) continue;
        dayPnl += sdata.pnl;
        filteredTrades += sdata.trades;
        dayActive = true;
      }
      if (dayActive) { dailySeries.push(dayPnl); activeDates.push(date); }
    }

    return deriveMetrics(dailySeries, filteredTrades, activeDates);
  });

  // ── Derived: overall metrics cards ──────────────────────────────────────────
  vm.overallMetrics = ko.computed(function() {
    const m = vm.filteredMetrics();
    if (!m) return [];

    const pf = m.profit_factor != null ? m.profit_factor.toFixed(2) : '∞';
    const pfClass = m.profit_factor == null || m.profit_factor >= 1 ? 'text-success' : 'text-danger';
    const sharpeClass = (m.sharpe_ratio || 0) >= 0 ? 'text-success' : 'text-danger';
    const kellyClass = (m.kelly_ratio || 0) > 0 ? 'text-success' : 'text-danger';
    const pnlClass = (m.total_pnl || 0) >= 0 ? 'text-success' : 'text-danger';
    const winRateClass = (m.win_rate || 0) >= 50 ? 'text-success' : 'text-danger';
    const weeklyClass = (m.weekly_success_rate || 0) >= 50 ? 'text-success' : 'text-danger';
    const bestPnl = m.best_day_pnl != null ? fmtPnl(m.best_day_pnl) : '-';
    const worstPnl = m.worst_day_pnl != null ? fmtPnl(m.worst_day_pnl) : '-';

    return [
      { label: 'Total PnL',            value: fmtPnl(m.total_pnl || 0),                           cls: pnlClass },
      { label: 'Days Tested',          value: (m.profitable_days || 0) + '/' + (m.days_tested || 0), cls: '' },
      { label: 'Win Rate',             value: (m.win_rate || 0).toFixed(2) + '%',                  cls: winRateClass },
      { label: 'Weekly Win Rate',      value: (m.winning_weeks || 0) + '/' + (m.total_weeks || 0) + ' (' + (m.weekly_success_rate || 0).toFixed(1) + '%)', cls: weeklyClass },
      { label: 'Total Trades',         value: String(m.total_trades || 0),                         cls: '' },
      { label: 'Kelly Ratio',          value: (m.kelly_ratio || 0).toFixed(4),                     cls: kellyClass },
      { label: 'Max Drawdown',         value: fmtPnl(m.max_drawdown || 0),                         cls: 'text-danger' },
      { label: 'Sharpe (annual)',      value: (m.sharpe_ratio || 0).toFixed(3),                    cls: sharpeClass },
      { label: 'Profit Factor',        value: pf,                                                   cls: pfClass },
      { label: 'Max Win Streak',       value: String(m.max_win_streak || 0),                       cls: 'text-success' },
      { label: 'Max Loss Streak',      value: String(m.max_loss_streak || 0),                      cls: 'text-danger' },
      { label: 'Best Day',             value: bestPnl,                                              cls: 'text-success' },
      { label: 'Worst Day',            value: worstPnl,                                             cls: 'text-danger' },
    ];
  });

  // ── Equity curve ─────────────────────────────────────────────────────────────
  vm.chartData = ko.computed(function() {
    const res = vm.results();
    if (!res || !res.daily_breakdown) return null;
    const map = vm._effectiveMap();  // tracked
    const strategies = vm.allStrategies();
    const sortedDates = Object.keys(res.daily_breakdown).sort();

    const cumByStrat = {};
    for (const s of strategies) cumByStrat[s] = 0;
    let cumTotal = 0;

    const traces = strategies.map(function(s) { return { name: s, y: [] }; });
    const totalTrace = { name: 'Total', y: [] };

    for (const date of sortedDates) {
      const dayData = res.daily_breakdown[date];
      let dayTotal = 0;
      for (let i = 0; i < strategies.length; i++) {
        const s = strategies[i];
        const sd = dayData[s];
        let pnl = 0;
        if (sd) {
          if (map[date + '|||' + s] !== false) pnl = sd.pnl;
        }
        cumByStrat[s] += pnl;
        dayTotal += pnl;
        traces[i].y.push(cumByStrat[s]);
      }
      cumTotal += dayTotal;
      totalTrace.y.push(cumTotal);
    }

    traces.push(totalTrace);

    let peak = null;
    const drawdownY = totalTrace.y.map(function(v) {
      if (peak === null || v > peak) peak = v;
      return Math.min(0, v - (peak || 0));
    });
    traces.push({ name: 'Drawdown', y: drawdownY, isDrawdown: true });

    return { dates: sortedDates, traces: traces };
  });

  // Chart visibility toggle + render trigger
  vm.toggleChart = function() {
    vm.chartVisible(!vm.chartVisible());
    if (vm.chartVisible()) {
      setTimeout(function() {
        const el = document.getElementById(vm._chartElId);
        if (el) Plotly.Plots.resize(el);
      }, 50);
    }
  };
  vm._chartElId = 'equity-chart-' + Math.random().toString(36).slice(2, 8);

  vm.chartTraceNames = ko.computed(function() {
    const data = vm.chartData();
    if (!data) return [];
    return data.traces.map(function(t) { return t.name; });
  });

  vm._chartTraceSelected = ko.observable({});
  vm.chartTraceNames.subscribe(function(names) {
    const sel = {};
    const prev = vm._chartTraceSelected();
    for (const name of names) sel[name] = prev[name] !== undefined ? prev[name] : true;
    vm._chartTraceSelected(sel);
  });

  vm.isChartTraceSelected = function(name) {
    return vm._chartTraceSelected()[name] !== false;
  };
  vm.toggleChartTrace = function(name) {
    const sel = Object.assign({}, vm._chartTraceSelected());
    sel[name] = !sel[name];
    vm._chartTraceSelected(sel);
  };

  vm.filteredChartData = ko.computed(function() {
    const data = vm.chartData();
    const sel = vm._chartTraceSelected();
    if (!data) return null;
    const traces = data.traces.filter(function(t) { return sel[t.name] !== false; });
    return { dates: data.dates, traces: traces };
  });

  // ── Monthly PnL breakdown ────────────────────────────────────────────────────
  const MONTH_NAMES = ['January','February','March','April','May','June',
                       'July','August','September','October','November','December'];

  vm.monthlyData = ko.computed(function() {
    const res = vm.results();
    if (!res || !res.daily_breakdown) return null;
    const map = vm._effectiveMap();  // tracked
    const byYearMonth = {};

    for (const [date, dayData] of Object.entries(res.daily_breakdown)) {
      const year = date.slice(0, 4);
      const monthIdx = parseInt(date.slice(5, 7), 10) - 1;
      if (!byYearMonth[year]) byYearMonth[year] = {};
      if (byYearMonth[year][monthIdx] === undefined) byYearMonth[year][monthIdx] = 0;
      let dayTotal = 0;
      for (const [strat, sdata] of Object.entries(dayData)) {
        if (strat.startsWith('_')) continue;
        if (map[date + '|||' + strat] === false) continue;
        dayTotal += sdata.pnl;
      }
      byYearMonth[year][monthIdx] += dayTotal;
    }

    const monthIndices = [0,1,2,3,4,5,6,7,8,9,10,11];
    const years = Object.keys(byYearMonth).sort().map(function(year) {
      const mMap = byYearMonth[year];
      let yearTotal = 0;
      const months = monthIndices.map(function(m) {
        const v = mMap[m] !== undefined ? mMap[m] : null;
        if (v !== null) yearTotal += v;
        return v;
      });
      return { year: year, months: months, yearTotal: yearTotal };
    });

    return { monthNames: monthIndices.map(function(m) { return MONTH_NAMES[m]; }), years: years };
  });

  vm.toggleMonthly = function() { vm.monthlyVisible(!vm.monthlyVisible()); };

  // ── Weekly PnL breakdown ────────────────────────────────────────────────────
  vm.weeklyData = ko.computed(function() {
    const res = vm.results();
    if (!res || !res.daily_breakdown) return null;
    const map = vm._effectiveMap();  // tracked

    const byWeek = {};
    for (const [date, dayData] of Object.entries(res.daily_breakdown)) {
      const weekKey = isoWeekKey(date);
      let dayTotal = 0;
      for (const [strat, sdata] of Object.entries(dayData)) {
        if (strat.startsWith('_')) continue;
        if (map[date + '|||' + strat] === false) continue;
        dayTotal += sdata.pnl;
      }
      if (!byWeek[weekKey]) byWeek[weekKey] = { pnl: 0, days: 0, dates: [] };
      byWeek[weekKey].pnl += dayTotal;
      byWeek[weekKey].days += 1;
      byWeek[weekKey].dates.push(date);
    }

    const sortedKeys = Object.keys(byWeek).sort();
    const weeks = sortedKeys.map(function(wk) {
      const w = byWeek[wk];
      w.dates.sort();
      return {
        week: wk,
        dateRange: w.dates[0] + ' — ' + w.dates[w.dates.length - 1],
        pnl: w.pnl,
        days: w.days,
      };
    });

    const totalPnl = weeks.reduce(function(s, w) { return s + w.pnl; }, 0);
    const winWeeks = weeks.filter(function(w) { return w.pnl > 0; }).length;
    return { weeks: weeks, totalPnl: totalPnl, winWeeks: winWeeks, totalWeeks: weeks.length };
  });
  vm.toggleWeekly = function() { vm.weeklyVisible(!vm.weeklyVisible()); };

  // ── Weekday breakdown (with aggregator checkboxes) ───────────────────────────
  vm.weekdayRows = ko.computed(function() {
    const res = vm.results();
    if (!res || !res.daily_breakdown) return [];
    const map = vm._effectiveMap();  // tracked

    const byDay = {};
    for (const label of WEEKDAY_ORDER) {
      byDay[label] = { pnl: 0, days: 0, wins: 0, winning_pnl: 0, losing_pnl: 0, checkedDates: 0, totalDates: 0, allTrades: 0, uncheckedTrades: 0 };
    }

    // Group dates by weekday so we can compute aggregator state
    const datesByWeekday = {};
    for (const date of Object.keys(res.daily_breakdown)) {
      const label = WEEKDAYS[new Date(date).getUTCDay()];
      if (!byDay[label]) continue;
      if (!datesByWeekday[label]) datesByWeekday[label] = [];
      datesByWeekday[label].push(date);
    }

    for (const label of WEEKDAY_ORDER) {
      const dates = datesByWeekday[label] || [];
      for (const date of dates) {
        const dayData = res.daily_breakdown[date];
        // A date is "checked" if at least one strategy on that date is included
        const stratsOnDay = Object.keys(dayData).filter(function(k) { return !k.startsWith('_'); });
        const allStratKeys = stratsOnDay.map(function(s) { return date + '|||' + s; });
        const checkedStrats = allStratKeys.filter(function(k) { return map[k] !== false; });

        let dayAllTrades = 0, dayUncheckedTrades = 0, dayTotal = 0;
        for (const s of stratsOnDay) {
          const sd = dayData[s];
          dayAllTrades += sd.trades;
          if (map[date + '|||' + s] === false) { dayUncheckedTrades += sd.trades; continue; }
          dayTotal += sd.pnl;
        }
        byDay[label].allTrades += dayAllTrades;
        byDay[label].uncheckedTrades += dayUncheckedTrades;

        if (dayAllTrades === 0) continue;  // skip zero-trade dates (e.g. missing JSON for that day)
        byDay[label].totalDates++;

        if (checkedStrats.length === 0) continue;  // date fully excluded
        byDay[label].checkedDates++;
        byDay[label].pnl += dayTotal;
        byDay[label].days += 1;
        if (dayTotal > 0) { byDay[label].wins += 1; byDay[label].winning_pnl += dayTotal; }
        else if (dayTotal < 0) { byDay[label].losing_pnl += Math.abs(dayTotal); }
      }
    }

    return WEEKDAY_ORDER.map(function(label) {
      const t = byDay[label];
      if (t.allTrades === 0) return null;
      const { avg_win, avg_loss, kelly_ratio } = calcKelly(t.wins, t.days, t.winning_pnl, t.losing_pnl);
      const allChecked = t.checkedDates === t.totalDates && t.totalDates > 0;
      const someChecked = t.checkedDates > 0 && t.checkedDates < t.totalDates && t.uncheckedTrades > 0;
      const dates = datesByWeekday[label] || [];

      function toggleWeekday() {
        const newVal = !allChecked;
        setChecked(function(key) {
          const date = key.split('|||')[0];
          return dates.indexOf(date) !== -1;
        }, newVal);
      }

      return {
        label: label,
        totalPnl: t.pnl,
        totalDays: t.days,
        profitable_days: t.wins,
        win_rate: t.days > 0 ? (t.wins / t.days * 100) : 0,
        avg_pnl: t.days > 0 ? t.pnl / t.days : 0,
        avg_win: avg_win,
        avg_loss: avg_loss,
        kelly_ratio: kelly_ratio,
        weekdayCheckbox: { checked: allChecked, indeterminate: someChecked, toggle: toggleWeekday },
      };
    }).filter(Boolean);
  });

  vm.toggleWeekday = function() { vm.weekdayVisible(!vm.weekdayVisible()); };

  // ── DTE tables ────────────────────────────────────────────────────────────────

  // Total row for a DTE table: sums only the selected rows' raw accumulators.
  // rows must have { selected computed, _pnl, _trades, _days, _wins, _winning_pnl, _losing_pnl, _dayPnls }
  function makeDteTotalRow(rows) {
    return ko.computed(function() {
      let pnl = 0, trades = 0, days = 0, wins = 0, winning_pnl = 0, losing_pnl = 0;
      const dayPnls = [];
      for (const r of rows) {
        if (!r.dteCheckbox.checked && !r.dteCheckbox.indeterminate) continue;
        pnl += r._pnl; trades += r._trades; days += r._days;
        wins += r._wins; winning_pnl += r._winning_pnl; losing_pnl += r._losing_pnl;
        if (r._dayPnls) dayPnls.push.apply(dayPnls, r._dayPnls);
      }
      return calcDteRowMetrics(days, wins, winning_pnl, losing_pnl, trades, pnl, dayPnls);
    });
  }

  // ── Per-strategy DTE tables (primary filter checkboxes) ──────────────────────
  // Each (strat, dte) row is PRIMARY: its checkbox directly drives _dateStratChecked.
  vm.dteStratEntries = ko.computed(function() {
    const res = vm.results();
    if (!res || !res.dte_breakdown) return [];
    const map = vm._effectiveMap();  // tracked

    // Collect all known (strat, dte) combinations from dte_breakdown keys
    const stratDtePairs = {};
    for (const strat of Object.keys(res.dte_breakdown)) {
      if (strat === '_TOTAL') continue;
      stratDtePairs[strat] = Object.keys(res.dte_breakdown[strat]).map(Number);
    }

    return Object.keys(stratDtePairs).sort().map(function(strat) {
      const rows = stratDtePairs[strat].sort(function(a, b) { return a - b; }).map(function(dte) {
        // Compute filtered stats from daily_breakdown for this (strat, dte) combination
        let pnl = 0, trades = 0, days = 0, wins = 0, winning_pnl = 0, losing_pnl = 0;
        const matchingKeys = [];
        const dayPnls = [];

        let allTrades = 0, uncheckedTrades = 0;
        for (const [date, dayData] of Object.entries(res.daily_breakdown)) {
          const sd = dayData[strat];
          if (!sd || sd.days_to_expiry !== dte) continue;
          const key = date + '|||' + strat;
          allTrades += sd.trades;
          if (sd.trades > 0) matchingKeys.push(key);
          if (map[key] === false) { uncheckedTrades += sd.trades; continue; }
          pnl += sd.pnl;
          trades += sd.trades;
          days += 1;
          dayPnls.push(sd.pnl);
          if (sd.pnl > 0) { wins += 1; winning_pnl += sd.pnl; }
          else if (sd.pnl < 0) { losing_pnl += Math.abs(sd.pnl); }
        }

        if (allTrades === 0) return null;

        const checkedCount = matchingKeys.filter(function(k) { return map[k] !== false; }).length;
        const allChecked = checkedCount === matchingKeys.length && matchingKeys.length > 0;
        const someChecked = checkedCount > 0 && checkedCount < matchingKeys.length && uncheckedTrades > 0;

        function toggleDteStrat() {
          const newVal = !allChecked;
          setChecked(function(key) { return matchingKeys.indexOf(key) !== -1; }, newVal);
        }

        const metrics = calcDteRowMetrics(days, wins, winning_pnl, losing_pnl, trades, pnl, dayPnls);
        return Object.assign({
          dteLabel: dte === 0 ? '0 (Expiry)' : String(dte),
          dteValue: dte,
          dteCheckbox: { checked: allChecked, indeterminate: someChecked, toggle: toggleDteStrat },
          _pnl: pnl, _trades: trades, _days: days,
          _wins: wins, _winning_pnl: winning_pnl, _losing_pnl: losing_pnl, _dayPnls: dayPnls,
        }, metrics);
      }).filter(Boolean);
      return { label: strat, stratKey: strat, rows, totalRow: makeDteTotalRow(rows) };
    });
  });

  vm.toggleDteStrat = function() { vm.dteStratVisible(!vm.dteStratVisible()); };

  // ── DTE-wise Strategy tables ──────────────────────────────────────────────────
  vm.dteWiseStratEntries = ko.computed(function() {
    const res = vm.results();
    if (!res || !res.daily_breakdown) return [];
    const map = vm._effectiveMap();  // tracked — used for display/aggregation
    const rawMap = vm._dateStratChecked();  // used for checkbox intent (unaffected by excludeTop2)

    // Collect all (dte, strat) combinations with actual trades
    const dteStratTrades = {};  // dteStratTrades[dte][strat] = total trades (for filtering zero-trade rows)
    for (const [, dayData] of Object.entries(res.daily_breakdown)) {
      for (const [strat, sdata] of Object.entries(dayData)) {
        if (strat.startsWith('_')) continue;
        const dte = sdata.days_to_expiry;
        if (dte == null) continue;
        if (!dteStratTrades[dte]) dteStratTrades[dte] = {};
        dteStratTrades[dte][strat] = (dteStratTrades[dte][strat] || 0) + sdata.trades;
      }
    }

    const dteKeys = Object.keys(dteStratTrades).map(Number)
      .filter(function(dte) { return Object.values(dteStratTrades[dte]).some(function(t) { return t > 0; }); });
    if (dteKeys.length === 0) return [];

    return dteKeys.sort(function(a, b) { return a - b; }).map(function(dte) {
      const stratKeys = Object.keys(dteStratTrades[dte]).filter(function(s) { return dteStratTrades[dte][s] > 0; }).sort();

      const sortCol = ko.observable('total_pnl');
      const sortDir = ko.observable(-1);
      function sortBy(col) {
        if (sortCol() === col) { sortDir(sortDir() * -1); } else { sortCol(col); sortDir(-1); }
      }
      function sortArrow(col) { return sortCol() === col ? (sortDir() === 1 ? ' ▲' : ' ▼') : ''; }

      const excludeTop2 = getDteExcludeTop2(dte);

      // Rows for this DTE: map is _effectiveMap which already applies excludeTop2 overrides
      const filteredRows = ko.computed(function() {
        return stratKeys.map(function(strat) {
          let pnl = 0, trades = 0, days = 0, wins = 0, winning_pnl = 0, losing_pnl = 0;
          const dayPnls = [];
          const matchingKeys = [];
          let allTrades = 0, uncheckedTrades = 0;

          for (const [date, dayData] of Object.entries(res.daily_breakdown)) {
            const sd = dayData[strat];
            if (!sd || sd.days_to_expiry !== dte) continue;
            const key = date + '|||' + strat;
            allTrades += sd.trades;
            if (sd.trades > 0) matchingKeys.push(key);
            if (map[key] === false) { uncheckedTrades += sd.trades; continue; }
            if (sd.trades === 0) continue;
            pnl += sd.pnl; trades += sd.trades; days += 1;
            dayPnls.push(sd.pnl);
            if (sd.pnl > 0) { wins += 1; winning_pnl += sd.pnl; }
            else if (sd.pnl < 0) { losing_pnl += Math.abs(sd.pnl); }
          }

          if (allTrades === 0) return null;

          const checkedCount = matchingKeys.filter(function(k) { return rawMap[k] !== false; }).length;
          const allChecked = checkedCount === matchingKeys.length && matchingKeys.length > 0;
          const someChecked = checkedCount > 0 && checkedCount < matchingKeys.length;

          function toggleDteWiseStrat() {
            const newVal = !allChecked;
            setChecked(function(key) { return matchingKeys.indexOf(key) !== -1; }, newVal);
          }

          const metrics = calcDteRowMetrics(days, wins, winning_pnl, losing_pnl, trades, pnl, dayPnls);
          return Object.assign({
            stratLabel: strat,
            dteCheckbox: { checked: allChecked, indeterminate: someChecked, toggle: toggleDteWiseStrat },
            _pnl: pnl, _trades: trades, _days: days,
            _wins: wins, _winning_pnl: winning_pnl, _losing_pnl: losing_pnl, _dayPnls: dayPnls,
          }, metrics);
        }).filter(Boolean);
      });

      const filteredTotalRow = ko.computed(function() {
        let pnl = 0, trades = 0, days = 0, wins = 0, winning_pnl = 0, losing_pnl = 0;
        const dayPnls = [];
        for (const r of filteredRows()) {
          pnl += r._pnl; trades += r._trades; days += r._days;
          wins += r._wins; winning_pnl += r._winning_pnl; losing_pnl += r._losing_pnl;
          if (r._dayPnls) dayPnls.push.apply(dayPnls, r._dayPnls);
        }
        return calcDteRowMetrics(days, wins, winning_pnl, losing_pnl, trades, pnl, dayPnls);
      });

      const sortedRows = ko.computed(function() {
        const col = sortCol(), dir = sortDir();
        return filteredRows().slice().sort(function(a, b) {
          const av = a[col] != null ? a[col] : 0;
          const bv = b[col] != null ? b[col] : 0;
          if (typeof av === 'string') return av < bv ? -dir : av > bv ? dir : 0;
          return (av - bv) * dir;
        });
      });

      return {
        dteLabel: dte === 0 ? '0 (Expiry)' : String(dte),
        dteValue: dte,
        sortedRows: sortedRows,
        sortBy: sortBy,
        sortArrow: sortArrow,
        excludeTop2: excludeTop2,
        filteredTotalRow: filteredTotalRow,
      };
    });
  });

  vm.toggleDteWiseStrat = function() { vm.dteWiseStratVisible(!vm.dteWiseStratVisible()); };

  // ── Overall DTE table (aggregator) ───────────────────────────────────────────
  // Each DTE row aggregates across all strategies for that DTE.
  vm.dteEntries = ko.computed(function() {
    const res = vm.results();
    if (!res || !res.daily_breakdown) return [];
    const map = vm._effectiveMap();  // tracked

    // Build filtered stats per DTE — only include checked (date, strat) pairs.
    // Also track total trades per DTE across all keys (for filter + indeterminate logic).
    const dteRaw = {};
    const dteTotalTrades = {};  // all trades per DTE regardless of checked state
    for (const [date, dayData] of Object.entries(res.daily_breakdown)) {
      const date_dte_totals = {};
      for (const [strat, sdata] of Object.entries(dayData)) {
        if (strat.startsWith('_')) continue;
        const dte = sdata.days_to_expiry;
        if (dte == null) continue;
        dteTotalTrades[dte] = (dteTotalTrades[dte] || 0) + sdata.trades;
        if (map[date + '|||' + strat] === false) continue;
        if (!date_dte_totals[dte]) date_dte_totals[dte] = { pnl: 0, trades: 0 };
        date_dte_totals[dte].pnl += sdata.pnl;
        date_dte_totals[dte].trades += sdata.trades;
      }
      for (const [dte, totals] of Object.entries(date_dte_totals)) {
        if (!dteRaw[dte]) dteRaw[dte] = { pnl: 0, trades: 0, days: 0, wins: 0, winning_pnl: 0, losing_pnl: 0, dayPnls: [] };
        dteRaw[dte].pnl += totals.pnl;
        dteRaw[dte].trades += totals.trades;
        dteRaw[dte].days += 1;
        dteRaw[dte].dayPnls.push(totals.pnl);
        if (totals.pnl > 0) { dteRaw[dte].wins += 1; dteRaw[dte].winning_pnl += totals.pnl; }
        else if (totals.pnl < 0) { dteRaw[dte].losing_pnl += Math.abs(totals.pnl); }
      }
    }

    // Only render DTEs that exist in daily_breakdown with trades
    const dteKeys = Object.keys(dteTotalTrades).map(Number)
      .filter(function(dte) { return dteTotalTrades[dte] > 0; });
    if (dteKeys.length === 0) return [];

    const rows = dteKeys.sort(function(a, b) { return a - b; }).map(function(dte) {
      const row = dteRaw[dte] || { pnl: 0, trades: 0, days: 0, wins: 0, winning_pnl: 0, losing_pnl: 0, dayPnls: [] };
      // Aggregator state: find all (date, strat) keys for this DTE that have actual trades
      const matchingKeys = Object.keys(map).filter(function(key) {
        const parts = key.split('|||');
        const dayData = res.daily_breakdown[parts[0]];
        const sd = dayData && dayData[parts[1]];
        return sd && sd.days_to_expiry === dte && sd.trades > 0;
      });
      const checkedCount = matchingKeys.filter(function(k) { return map[k] !== false; }).length;
      const uncheckedTrades = matchingKeys.filter(function(k) { return map[k] === false; })
        .reduce(function(sum, k) {
          const parts = k.split('|||');
          return sum + (res.daily_breakdown[parts[0]][parts[1]].trades || 0);
        }, 0);
      const allChecked = checkedCount === matchingKeys.length && matchingKeys.length > 0;
      const someChecked = checkedCount > 0 && checkedCount < matchingKeys.length && uncheckedTrades > 0;

      function toggleDte() {
        const newVal = !allChecked;
        setChecked(function(key) { return matchingKeys.indexOf(key) !== -1; }, newVal);
      }

      const metrics = calcDteRowMetrics(row.days, row.wins, row.winning_pnl, row.losing_pnl, row.trades, row.pnl, row.dayPnls);
      return Object.assign({
        dteLabel: dte === 0 ? '0 (Expiry)' : String(dte),
        dteValue: dte,
        dteCheckbox: { checked: allChecked, indeterminate: someChecked, toggle: toggleDte },
        _pnl: row.pnl, _trades: row.trades, _days: row.days,
        _wins: row.wins, _winning_pnl: row.winning_pnl, _losing_pnl: row.losing_pnl, _dayPnls: row.dayPnls,
      }, metrics);
    });
    return [{ label: 'All Strategies Combined', stratKey: '_TOTAL', rows: rows, totalRow: makeDteTotalRow(rows) }];
  });

  // ── Daily PnL by Strategy (with date checkboxes) ─────────────────────────────
  vm.sortedDays = ko.computed(function() {
    if (!vm.dailyVisible()) return [];
    const res = vm.results();
    if (!res || !res.daily_breakdown) return [];
    const map = vm._effectiveMap();  // tracked
    const strategies = vm.allStrategies();
    const col = vm.sortCol();
    const dir = vm.sortDir();

    const rows = Object.keys(res.daily_breakdown).sort().map(function(date) {
      const dayData = res.daily_breakdown[date];
      const weekday = WEEKDAYS[new Date(date).getUTCDay()];
      let dte = null;
      for (const s of Object.values(dayData)) {
        if (s && s.days_to_expiry != null) { dte = s.days_to_expiry; break; }
      }

      // Date-level aggregator checkbox state
      const stratKeysForDate = strategies.filter(function(s) { return !!dayData[s]; })
        .map(function(s) { return date + '|||' + s; });
      const checkedCount = stratKeysForDate.filter(function(k) { return map[k] !== false; }).length;
      const uncheckedTrades = stratKeysForDate.filter(function(k) { return map[k] === false; })
        .reduce(function(sum, k) { return sum + (dayData[k.split('|||')[1]].trades || 0); }, 0);
      const allChecked = checkedCount === stratKeysForDate.length && stratKeysForDate.length > 0;
      const someChecked = checkedCount > 0 && checkedCount < stratKeysForDate.length && uncheckedTrades > 0;

      function toggleDate() {
        const newVal = !allChecked;
        setChecked(function(key) { return stratKeysForDate.indexOf(key) !== -1; }, newVal);
      }

      const pnl = strategies.reduce(function(sum, s) {
        const sd = dayData[s];
        if (!sd) return sum;
        if (map[date + '|||' + s] === false) return sum;
        return sum + sd.pnl;
      }, 0);

      const stratCells = strategies.map(function(s) {
        const sd = dayData[s];
        if (!sd) return { strategy: s, pnl: 0, highest_pnl: 0, lowest_pnl: 0, qty: 0, run_id: dayData._run_id || '' };
        const included = map[date + '|||' + s] !== false;
        return {
          strategy: s,
          pnl: included ? sd.pnl : 0,
          highest_pnl: included ? (sd.highest_pnl || 0) : 0,
          lowest_pnl: included ? (sd.lowest_pnl || 0) : 0,
          qty: sd.qty || 0,
          run_id: dayData._run_id || '',
        };
      });

      return {
        date: date, weekday: weekday, dte: dte, pnl: pnl,
        highest_pnl: dayData._highest_pnl || 0,
        lowest_pnl: dayData._lowest_pnl || 0,
        stratCells: stratCells, run_id: dayData._run_id || '',
        dateCheckbox: { checked: allChecked, indeterminate: someChecked, toggle: toggleDate },
        isIncluded: allChecked || someChecked,
      };
    });

    rows.sort(function(a, b) {
      let av, bv;
      if (col === 'date')         { av = a.date; bv = b.date; }
      else if (col === 'weekday') { av = a.weekday; bv = b.weekday; }
      else if (col === 'dte')     { av = a.dte != null ? a.dte : 999; bv = b.dte != null ? b.dte : 999; }
      else if (col === 'total')   { av = a.pnl; bv = b.pnl; }
      else {
        const ca = a.stratCells.find(function(c) { return c.strategy === col; });
        const cb = b.stratCells.find(function(c) { return c.strategy === col; });
        av = ca ? ca.pnl : 0;
        bv = cb ? cb.pnl : 0;
      }
      return av < bv ? -dir : av > bv ? dir : 0;
    });

    return rows;
  });

  // Derived: per-strategy avg PnL for the Daily Average row (uses filteredMetrics strategy breakdown)
  vm.strategyAvgPnls = ko.computed(function() {
    if (!vm.dailyVisible()) return [];
    const res = vm.results();
    if (!res) return [];
    const map = vm._effectiveMap();  // tracked
    return vm.allStrategies().map(function(name) {
      let total_pnl = 0, total_days = 0;
      for (const [date, dayData] of Object.entries(res.daily_breakdown)) {
        const sd = dayData[name];
        if (!sd) continue;
        if (map[date + '|||' + name] === false) continue;
        total_pnl += sd.pnl;
        total_days++;
      }
      return total_days > 0 ? total_pnl / total_days : 0;
    });
  });

  // Open trade details modal for a specific date/run/strategy
  vm.showTradeDetails = function(date, runId, strategy) {
    vm.modalTitle('Trade Details - ' + date + ' - ' + strategy);
    vm.modalTrades([]);
    vm.modalSummary(null);
    vm.modalLoading(true);
    $('#tradeDetailsModal').modal('show');

    $.ajax({
      url: '/backtesting/' + shortCode + '/trades',
      type: 'GET',
      data: { date: date, run_id: runId, strategy: strategy },
      success: function(response) {
        vm.modalLoading(false);
        if (response.success && response.trades && response.trades.length > 0) {
          vm.modalTrades(response.trades);
          const trades = response.trades;
          const totalPnl = trades.reduce(function(s, t) { return s + (t.pnl || 0); }, 0);
          const winCount = trades.filter(function(t) { return (t.pnl || 0) > 0; }).length;
          const lossCount = trades.filter(function(t) { return (t.pnl || 0) < 0; }).length;
          const winRate = trades.length > 0 ? (winCount / trades.length * 100).toFixed(2) : '0.00';
          vm.modalSummary({
            count: trades.length,
            winCount: winCount,
            lossCount: lossCount,
            winRate: winRate,
            totalPnl: totalPnl,
          });
        } else {
          vm.modalTrades([]);
          vm.modalSummary({ empty: true });
        }
      },
      error: function(xhr) {
        vm.modalLoading(false);
        vm.modalTrades([]);
        vm.modalSummary({ error: xhr.responseJSON ? xhr.responseJSON.error : 'Failed to load trade details' });
      }
    });
  };

  return vm;
}
