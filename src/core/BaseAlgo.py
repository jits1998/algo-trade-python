import datetime
import json
import logging
import os
import threading
import time

from core.Controller import Controller
from instruments.Instruments import Instruments
from trademgmt.BacktestManager import BacktestManager
from trademgmt.TradeManager import TradeManager
from utils.Utils import Utils

# from Test import Test


class BaseAlgo:

    def __init__(self):
        self.strategyConfig = {}
        self.algoSL = (
            0  # Absolute daily loss limit on total PnL (negative, e.g. -50000). 0 = disabled.
        )
        self.algoTarget = 0  # Daily profit target per multiple (positive, e.g. 5000). 0 = disabled.
        self.algoTrailOffset = (
            0  # Fixed gap below peak PnL/multiple to set trail SL (e.g. 2500). 0 = disabled.
        )
        self.algoTrailStep = (
            0  # Min PnL/multiple rise above last peak before re-trailing (e.g. 1000).
        )
        self.algoTrailSL = (
            0  # Deprecated: use TradeManager.algoTrailSL instead. Kept for backcompat.
        )

    def runLive(self, accessToken, short_code, multiple):
        self.short_code = short_code
        self.multiple = int(multiple)
        if Utils.getTradeManager(short_code) is not None:
            logging.info("Algo has already started..")
            return

        logging.info("Starting Algo...")

        Utils.getQuestDBConnection(short_code)  # DDL only (create tables)

        if Controller.getBrokerLogin(short_code) is None:
            Controller.handleBrokerLogin({}, short_code)
            brokerLogin = Controller.getBrokerLogin(short_code)
            brokerLogin.setAccessToken(accessToken)
            Controller.getBrokerLogin(short_code).getBrokerHandle().set_access_token(accessToken)

        # start trade manager in a separate thread
        tm = TradeManager(
            name=short_code,
            args=(
                accessToken,
                self,
            ),
        )
        tm.algoInstance = self
        tm.start()

        # sleep for 2 seconds for TradeManager to get initialized
        while not Utils.getTradeManager(short_code).isReady:
            if not tm.is_alive():
                logging.info("Ending Algo...")
                return
            time.sleep(2)

        self.startStrategies(short_code, multiple, tm)

        logging.info("Algo started.")

    def runBacktest(
        self,
        accessToken,
        short_code,
        start_date,
        end_date,
        multiple,
        comment="",
        aggressive_mode=False,
        run_id=None,
    ):
        self.short_code = short_code
        self.multiple = int(multiple)
        from datetime import datetime, timedelta

        if run_id is None:
            run_id = datetime.now().strftime("%Y%m%dT%H%M%S")
        logging.info(f"Starting Backtest from {start_date} to {end_date}, run_id={run_id}")

        if Controller.getBrokerLogin(short_code) is None:
            Controller.handleBrokerLogin({}, short_code)
            brokerLogin = Controller.getBrokerLogin(short_code)
            brokerLogin.setAccessToken(accessToken)
            Controller.getBrokerLogin(short_code).getBrokerHandle().set_access_token(accessToken)

        instrumentsList = Instruments.fetchInstruments(short_code)

        if len(instrumentsList) == 0:
            logging.warning("Backtest not started - no instruments found.")
            return

        current_date = datetime.strptime(start_date, "%Y-%m-%d")
        end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")

        backtest_summary = {
            "run_id": run_id,
            "algo": self.__class__.__name__,
            "start_date": start_date,
            "end_date": end_date,
            "comment": comment,
            "total_pnl": 0,
            "total_trades": 0,
            "days_tested": 0,
            "days": [],  # [{date, pnl, strategies: {name: {days_to_expiry, highest_pnl, lowest_pnl}}}]
        }

        while current_date <= end_date_obj:
            date_str = current_date.strftime("%Y-%m-%d")

            if not Utils.isHoliday(current_date):
                logging.info(f"Running backtest for {date_str}")

                tm = BacktestManager(
                    name=short_code, args=(accessToken, self, date_str, run_id, short_code)
                )
                tm.aggressive_mode = aggressive_mode
                tm.setupBacktestEnvironment()

                run_log_file = os.path.join(
                    tm.backtest_results_dir, f"backtest_{date_str}_{run_id}.log"
                )
                run_log_handler = logging.FileHandler(run_log_file)
                run_log_handler.setFormatter(
                    logging.Formatter("%(asctime)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
                )
                logging.getLogger().addHandler(run_log_handler)

                Utils.getQuestDBConnection(short_code)  # DDL only (create tables)

                tm.loadIndexHistoricalData()

                if not tm.timestamp_maps.get("NIFTY 50"):
                    logging.warning(
                        f"No NIFTY 50 data for {date_str} - likely a market holiday. Skipping."
                    )
                    logging.getLogger().removeHandler(run_log_handler)
                    run_log_handler.close()
                    current_date += timedelta(days=1)
                    continue

                first_ts = min(tm.timestamp_maps["NIFTY 50"].keys())
                first_candle = tm.timestamp_maps["NIFTY 50"][first_ts]
                tm.symbolToCMPMap["exchange_timestamp"] = first_candle["date"]
                tm.symbolToCMPMap["NIFTY 50"] = first_candle["close"]

                self.startStrategies(short_code, multiple, tm)

                tm.start()
                tm.join()

                logging.info(f"Completed backtest for {date_str}")
                logging.getLogger().removeHandler(run_log_handler)
                run_log_handler.close()

                day_results = tm.backtest_results

                if day_results:
                    day_pnl = day_results.get("total_pnl", 0)
                    day_trades = day_results.get("total_trades", 0)
                    backtest_summary["total_pnl"] += day_pnl
                    backtest_summary["total_trades"] += day_trades
                    if day_trades > 0:
                        backtest_summary["days_tested"] += 1
                        # Load strategy metadata from the per-day JSON written by generateBacktestReport
                        report_file = os.path.join(
                            tm.backtest_results_dir, f"backtest_{date_str}_{run_id}.json"
                        )
                        day_report = {}
                        day_strategy_meta = {}
                        try:
                            with open(report_file, "r") as _f:
                                day_report = json.load(_f)
                                day_strategy_meta = day_report.get("strategies", {})
                        except Exception:
                            pass

                        backtest_summary["days"].append(
                            {
                                "date": date_str,
                                "pnl": day_pnl,
                                "highest_pnl": day_report.get("highest_pnl", 0),
                                "lowest_pnl": day_report.get("lowest_pnl", 0),
                                "strategies": day_strategy_meta,
                            }
                        )

            else:
                logging.info(f"Skipping {date_str} (holiday/weekend)")

            current_date += timedelta(days=1)

        self.backtest_results = backtest_summary

        # Write entry to the runs index for history
        self._appendToRunsIndex(short_code, backtest_summary)

        self.printBacktestSummary(backtest_summary, start_date, end_date)
        logging.info("Backtest completed.")

        return backtest_summary

    def _appendToRunsIndex(self, short_code, summary):
        import fcntl

        from config.Config import getServerConfig

        serverConfig = getServerConfig()
        index_file = os.path.join(serverConfig["deployDir"], "backtest_results", "runs_index.json")

        days = summary["days"]
        daily_pnls = [d.get("pnl", 0) for d in days]
        profitable = [p for p in daily_pnls if p > 0]
        losing = [p for p in daily_pnls if p < 0]
        n = len(daily_pnls)
        kelly_ratio = 0
        if profitable and losing:
            W = len(profitable) / n
            avg_win = sum(profitable) / len(profitable)
            avg_loss = abs(sum(losing) / len(losing))
            if avg_loss > 0:
                kelly_ratio = W - ((1 - W) / (avg_win / avg_loss))

        entry = {
            "run_id": summary["run_id"],
            "algo": summary["algo"],
            "start_date": summary["start_date"],
            "end_date": summary["end_date"],
            "comment": summary["comment"],
            "total_trades": summary.get("total_trades", 0),
            "kelly_ratio": round(kelly_ratio, 6),
            "days": days,
        }

        with open(index_file, "a+") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            f.seek(0)
            content = f.read()
            runs = json.loads(content) if content.strip() else []
            runs.append(entry)
            f.seek(0)
            f.truncate()
            json.dump(runs, f, indent=2, default=str)
            fcntl.flock(f, fcntl.LOCK_UN)

    def printBacktestSummary(self, summary, start_date, end_date):
        """
        Print comprehensive backtest summary with daily PnL and statistics
        """
        print("\n" + "=" * 80)
        print(f"BACKTEST SUMMARY: {start_date} to {end_date}")
        print("=" * 80)

        # Overall statistics
        print(f"\nOverall Statistics:")
        print(f"  Total Days Tested:  {summary['days_tested']}")
        print(f"  Total Trades:       {summary['total_trades']}")
        print(f"  Total PnL:          ₹{summary['total_pnl']:,.2f}")

        if summary["days_tested"] > 0:
            avg_daily_pnl = summary["total_pnl"] / summary["days_tested"]
            print(f"  Avg Daily PnL:      ₹{avg_daily_pnl:,.2f}")

        # Daily PnL breakdown
        if summary["days"]:
            print(f"\nDaily PnL Breakdown:")
            print(f"  {'Date':<12} {'PnL (₹)':>15} {'Status':>10}")
            print(f"  {'-'*12} {'-'*15} {'-'*10}")

            for day in summary["days"]:
                pnl = day["pnl"]
                status = "✓ Profit" if pnl > 0 else ("✗ Loss" if pnl < 0 else "- Break Even")
                print(f"  {day['date']:<12} {pnl:>15,.2f} {status:>10}")

        print("\n" + "=" * 80 + "\n")

    def startStrategies(self, short_code, multiple, tradeManager):
        pass

    def _shouldSkipStrategy(self, strategy, tradeManager):
        from trademgmt.BacktestManager import BacktestManager
        from trademgmt.ShadowManager import ShadowManager
        if isinstance(tradeManager, ShadowManager) and strategy.skip_in_shadow:
            return True
        if isinstance(tradeManager, BacktestManager) and strategy.skip_in_backtest:
            return True
        return False

    def startStrategy(
        self,
        strategy,
        short_code,
        multiple,
        tradeManager,
        run=[0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        **kwargs,
    ):
        if self._shouldSkipStrategy(strategy, tradeManager):
            logging.info('BaseAlgo: skipping %s for %s', strategy.__name__, tradeManager.__class__.__name__)
            return

        strategyInstance = strategy(short_code, multiple, tradeManager, **kwargs)
        self.strategyConfig[strategyInstance.getName()] = run
        tradeManager.startStrategyExecution(
            strategyInstance, short_code + "_" + strategyInstance.getName()
        )

    def startTimedStrategy(
        self,
        strategy,
        short_code,
        multiple,
        tradeManager,
        run=[0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        startTimestamp=None,
        **kwargs,
    ):
        if self._shouldSkipStrategy(strategy, tradeManager):
            logging.info('BaseAlgo: skipping %s for %s', strategy.__name__, tradeManager.__class__.__name__)
            return

        strategyInstance = strategy(
            short_code, multiple, tradeManager, startTimestamp=startTimestamp, **kwargs
        )
        self.strategyConfig[strategyInstance.getName()] = run
        tradeManager.startStrategyExecution(
            strategyInstance, short_code + "_" + strategyInstance.getName()
        )

    def getLots(self, strategyName, symbol, expiryDay, expiryType="weekly"):

        strategyLots = self.strategyConfig.get(strategyName, [0, -1, -1, -1, -1, -1, 0, 0, 0, 0])

        if expiryType == "monthly":
            if Utils.isTodayMonthlyExpiryDay(symbol, expiryDay):
                return strategyLots[0]
            noOfDaysBeforeExpiry = Utils.findNumberOfDaysBeforeMonthlyExpiryDay(expiryDay=expiryDay)
        else:
            if Utils.isTodayWeeklyExpiryDay(symbol, expiryDay):
                return strategyLots[0]
            noOfDaysBeforeExpiry = Utils.findNumberOfDaysBeforeWeeklyExpiryDay(symbol, expiryDay)

        if (
            noOfDaysBeforeExpiry <= len(strategyLots) - 1
            and strategyLots[-noOfDaysBeforeExpiry] > 0
        ):
            return strategyLots[-noOfDaysBeforeExpiry]

        # override the weekday run count
        if (
            noOfDaysBeforeExpiry <= len(strategyLots) - 1
            and strategyLots[-noOfDaysBeforeExpiry] < 0
        ):
            return 0

        # adding + 1 to set monday index as 1
        dayOfWeek = Utils.getExchangeTimestamp(self.short_code).weekday() + 1

        # this will handle the run condition during thread start by defaulting to -1, and thus wait in get Lots
        if dayOfWeek >= 1 and dayOfWeek <= 5:
            return strategyLots[dayOfWeek]

        logging.info(
            f"{strategyName}::getLots - No condition matched, returning 0. strategyLots: {strategyLots}"
        )
        return 0
