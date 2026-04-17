# Auto-Trader Project Instructions

## General Rules

- **Always confirm the exact fix before implementing.** When a bug or requirement is identified, do not assume what the fix should be. Ask for explicit confirmation of the approach before writing any code.
- **Never assume or guess.** Only state what is confirmed by code, logs, or data. If something is unknown, say so and investigate rather than speculating.

## Code Style

- **Avoid defensive `hasattr` and redundant guards.** Trust that objects are correctly initialised. If a method exists on a class, its dependencies are set in `__init__`. Don't check for attribute existence before using it unless there is a genuine reason the attribute might be absent (e.g. cross-class usage or optional config).
- **Avoid redundant conditional checks.** If a condition is always true in context (e.g. a method on `BacktestManager` where `is_backtest_mode` is always `True`), remove the check rather than keeping it "for safety".
- **Trust internal class invariants.** Don't add fallback branches for states that can't occur given the class structure.

## Files to Ignore

When making changes to the codebase, **always ignore files in `archived` folders**.

- Do not modify any files under `src/algos/archived/`
- Do not modify any files under `src/strategies/archived/`
- Do not include archived files in bulk updates or refactoring operations

These archived files are kept for reference only and should not be updated as part of ongoing development.

## Active Development Areas

Focus changes on:
- `src/algos/` (excluding `archived/` subfolder)
- `src/strategies/` (excluding `archived/` subfolder)
- `src/core/`
- `src/trademgmt/`
- Other non-archived directories

## Backtest Results Storage

**Config:** `config/server.json` — `deployDir` is the base. Backtest results live at `{deployDir}/backtest_results/`. There is currently no separate `backtestResultsDir` key; code constructs the path by appending `backtest_results` to `deployDir`.

**Run ID format:** `YYYYMMDDTHHMMSS` (e.g. `20260216T103045`), generated via `datetime.now().strftime('%Y%m%dT%H%M%S')` in `BacktestingAPI.post()` and `BaseAlgo`.

**File layout per run:**
```
{deployDir}/backtest_results/
  backtest_{date}_{run_id}.json    ← one file per day per run
  backtest_{date}_{run_id}.log     ← log output for that day
  runs_index.json                  ← summary array of all runs (append-only, file-locked)
```

For a 5-day run the same `run_id` appears across all daily files, grouping them together.

**Per-day JSON structure** (`backtest_{date}_{run_id}.json`):
- `date`, `strategies` (per-strategy stats: pnl, win_rate, kelly_ratio, etc.), `total`, `daily_breakdown`, `raw_trades`
- `raw_trades` array: `symbol`, `strategy`, `entry`, `exit`, `pnl`, `brokerage`, `date`, `entry_time`, `exit_time`, `exit_reason`

**`runs_index.json`:** Array of run summaries — `run_id`, `algo`, `start_date`, `end_date`, `comment`, `total_pnl`, `days_tested`, `total_trades`, `win_count`, `loss_count`, `profitable_days`, `win_rate`, `strategies`, `days`.

**Key code locations:**
- `src/restapis/BacktestingAPI.py` — generates `run_id`, initiates run
- `src/trademgmt/BacktestManager.py` — `setupBacktestEnvironment()`, `generateBacktestReport()` (writes per-day JSON)
- `src/core/BaseAlgo.py` — `_appendToRunsIndex()` (appends to `runs_index.json`), `runBacktestForDay()`

## NSE Expiry

NSE (National Stock Exchange) **weekly expiry is on Tuesday** (changed from Thursday). Keep this in mind when writing or reviewing expiry-related logic in strategies and algos.

## QuestDB Access

**IMPORTANT: Always use the `candle-query` skill before writing any SQL against `historical_candles`.** The correct column names are `ts` (not `timestamp`), `trading_symbol` (not `symbol`), and QuestDB has limitations (no `HAVING`, no window functions). Using wrong column names will cause query errors.

Query QuestDB using the **local `psql` client** (installed on dev machine). Do not use HTTP/curl or other clients.

**Credentials are centralized in `config/server.json`** under the `questDB` key. Refer to that file for current connection parameters (host, port, username, password, database).

**Connection command example:**
```bash
# Read credentials from config/server.json and connect
PGPASSWORD=$(jq -r '.questDB.password' config/server.json) \
  psql -h $(jq -r '.questDB.host' config/server.json) \
       -p $(jq -r '.questDB.port' config/server.json) \
       -U $(jq -r '.questDB.username' config/server.json) \
       -d $(jq -r '.questDB.database' config/server.json)
```

Or pass a command directly:
```bash
PGPASSWORD=$(jq -r '.questDB.password' config/server.json) \
  psql -h $(jq -r '.questDB.host' config/server.json) \
       -p $(jq -r '.questDB.port' config/server.json) \
       -U $(jq -r '.questDB.username' config/server.json) \
       -d $(jq -r '.questDB.database' config/server.json) \
       -c "SELECT * FROM historical_candles LIMIT 5;"
```

**Python code** reads these credentials from `config/server.json` automatically — see `src/utils/Utils.py::getQuestDBConnection()` and `src/restapis/ChartAPI.py`.

(QuestDB exposes a Postgres-compatible endpoint on the configured port.)
