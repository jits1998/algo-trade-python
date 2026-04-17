from flask.views import MethodView
from flask import render_template, request, session, jsonify, redirect, url_for
from config.Config import getBrokerAppConfig
import os
import logging
import importlib
import threading
import json
from datetime import datetime

_backtest_jobs = {}  # run_id -> {'status': 'running'|'done'|'error', 'results': ..., 'error': ...}


class BacktestingAPI(MethodView):
    def get(self, short_code):
        # Check authentication
        if not session.get('short_code', None) == short_code or \
           session.get('access_token', None) is None:
            return redirect(url_for('home', short_code=short_code))

        # Get list of available algos
        algos_dir = os.path.join(os.path.dirname(
            os.path.dirname(__file__)), 'algos')
        available_algos = []

        if os.path.exists(algos_dir):
            for file in os.listdir(algos_dir):
                if file.endswith('.py') and not file.startswith('__'):
                    algo_name = file[:-3]  # Remove .py extension
                    available_algos.append(algo_name)

        available_algos.sort()

        # Render the backtesting template
        return render_template('backtesting.html',
                               short_code=short_code,
                               available_algos=available_algos,
                               multiple=getBrokerAppConfig(short_code).get("multiple", 1))

    def post(self, short_code):
        # Check authentication
        if not session.get('short_code', None) == short_code or \
           session.get('access_token', None) is None:
            return jsonify({"error": "Authentication required", "redirect": url_for('home', short_code=short_code)}), 401

        # Get parameters from request
        algo = request.form.get('algo')
        start_date = request.form.get('start_date')
        end_date = request.form.get('end_date')
        comment = request.form.get('comment', '')
        aggressive_mode = request.form.get('aggressive_mode') == 'on'

        # Validate parameters
        if not algo:
            return jsonify({"error": "Algo parameter is required"}), 400
        if not start_date:
            return jsonify({"error": "Start date is required"}), 400
        if not end_date:
            return jsonify({"error": "End date is required"}), 400
        if not comment:
            return jsonify({"error": "Comment is required"}), 400

        run_id = datetime.now().strftime('%Y%m%dT%H%M%S')
        logging.info(
            f'Backtesting request: run_id={run_id}, algo={algo}, start_date={start_date}, end_date={end_date}, comment={comment}, aggressive_mode={aggressive_mode}')

        algoType = algo
        algoConfigModule = importlib.import_module(
            'algos.' + algoType, algoType)
        algoConfigClass = getattr(algoConfigModule, algoType)

        algo_instance = algoConfigClass()
        access_token = session['access_token']
        short_code = session['short_code']
        multiple = getBrokerAppConfig(short_code).get("multiple", 1)

        from utils.Utils import Utils
        if Utils.getTradeManager(short_code) is not None:
            return jsonify({"error": "A TradeManager is already running for this account. Cannot start a backtest concurrently."}), 409

        _backtest_jobs[run_id] = {'status': 'running'}

        def run_backtest():
            try:
                algo_instance.runBacktest(access_token, short_code, start_date, end_date, multiple, comment, aggressive_mode, run_id=run_id)
                _backtest_jobs[run_id] = {'status': 'done', 'results': algo_instance.backtest_results}
            except Exception as e:
                logging.error(f'Backtest run_id={run_id} failed: {e}')
                _backtest_jobs[run_id] = {'status': 'error', 'error': str(e)}

        threading.Thread(target=run_backtest, name=f"Backtest-{run_id}", daemon=True).start()

        return jsonify({"run_id": run_id, "status": "running"})


class BacktestStatusAPI(MethodView):
    def get(self, short_code, run_id):
        if not session.get('short_code', None) == short_code or \
           session.get('access_token', None) is None:
            return jsonify({"error": "Authentication required"}), 401

        job = _backtest_jobs.get(run_id)
        if job is None:
            return jsonify({"error": "Unknown run_id"}), 404

        return jsonify(job)


class BacktestTradesAPI(MethodView):
    def get(self, short_code):
        # Check authentication
        if not session.get('short_code', None) == short_code or \
           session.get('access_token', None) is None:
            return jsonify({"error": "Authentication required"}), 401

        run_id = request.args.get('run_id')
        date = request.args.get('date')      # optional filter
        strategy = request.args.get('strategy')  # optional filter

        if not run_id:
            return jsonify({"error": "run_id parameter is required"}), 400

        from config.Config import getServerConfig
        import csv as csv_module
        serverConfig = getServerConfig()
        backtest_results_dir = os.path.join(serverConfig['deployDir'], 'backtest_results')
        csv_file = os.path.join(backtest_results_dir, f'trades_{run_id}.csv')

        if not os.path.exists(csv_file):
            return jsonify({"error": f"No trades CSV found for run {run_id}"}), 404

        try:
            trades = []
            with open(csv_file, 'r', newline='') as f:
                reader = csv_module.DictReader(f)
                for row in reader:
                    if date and row.get('Entry-Date') != date:
                        continue
                    if strategy and strategy != 'TOTAL' and row.get('Strategy') != strategy:
                        continue
                    # Normalise numeric fields
                    trades.append({
                        'symbol': row.get('Symbol', ''),
                        'strategy': row.get('Strategy', ''),
                        'date': row.get('Entry-Date', ''),
                        'entry_time': row.get('Entry-Time', ''),
                        'exit_time': row.get('ExitTime', ''),
                        'entry': float(row['Entry-Price']) if row.get('Entry-Price') else 0,
                        'exit': float(row['ExitPrice']) if row.get('ExitPrice') else 0,
                        'pnl': float(row['P/L']) if row.get('P/L') else 0,
                        'qty': int(row['Quantity']) if row.get('Quantity') else 0,
                        'direction': row.get('Position', ''),
                        'instrument_kind': row.get('Instrument-Kind', ''),
                        'strike_price': row.get('StrikePrice', ''),
                        'expiry_date': row.get('ExpiryDate', ''),
                        'pnl_pct': float(row['P/L-Percentage']) if row.get('P/L-Percentage') else 0,
                        'exit_reason': row.get('Remarks[exit reason]', ''),
                    })

            return jsonify({
                "success": True,
                "run_id": run_id,
                "date": date or "All",
                "strategy": strategy or "All",
                "trades": trades,
            })

        except Exception as e:
            logging.error(f'Error loading trade details: {str(e)}')
            return jsonify({"error": f"Failed to load trade details: {str(e)}"}), 500


class BacktestDeleteAPI(MethodView):
    def delete(self, short_code, run_id):
        if not session.get('short_code', None) == short_code or \
           session.get('access_token', None) is None:
            return jsonify({"error": "Authentication required"}), 401

        import fcntl
        from config.Config import getServerConfig
        serverConfig = getServerConfig()
        backtest_results_dir = os.path.join(serverConfig['deployDir'], 'backtest_results')
        index_file = os.path.join(backtest_results_dir, 'runs_index.json')

        if not os.path.exists(index_file):
            return jsonify({"error": "No runs index found"}), 404

        try:
            with open(index_file, 'r+') as f:
                fcntl.flock(f, fcntl.LOCK_EX)
                runs = json.load(f)
                original_count = len(runs)
                runs = [r for r in runs if r.get('run_id') != run_id]
                if len(runs) == original_count:
                    fcntl.flock(f, fcntl.LOCK_UN)
                    return jsonify({"error": f"Run {run_id} not found"}), 404
                f.seek(0)
                json.dump(runs, f, indent=2)
                f.truncate()
                fcntl.flock(f, fcntl.LOCK_UN)
        except Exception as e:
            logging.error(f'Error deleting run {run_id}: {str(e)}')
            return jsonify({"error": f"Failed to delete run: {str(e)}"}), 500

        # Delete associated per-day files and run CSV
        deleted_files = []
        for fname in os.listdir(backtest_results_dir):
            if fname.endswith(f'_{run_id}.json') or fname.endswith(f'_{run_id}.log') \
                    or fname == f'trades_{run_id}.csv':
                try:
                    os.remove(os.path.join(backtest_results_dir, fname))
                    deleted_files.append(fname)
                except Exception as e:
                    logging.warning(f'Could not delete file {fname}: {str(e)}')

        logging.info(f'Deleted backtest run {run_id}: removed {len(deleted_files)} files')
        return jsonify({"success": True, "run_id": run_id, "deleted_files": deleted_files})


class BacktestHistoryAPI(MethodView):
    def get(self, short_code):
        if not session.get('short_code', None) == short_code or \
           session.get('access_token', None) is None:
            return redirect(url_for('home', short_code=short_code))

        from config.Config import getServerConfig
        serverConfig = getServerConfig()
        index_file = os.path.join(serverConfig['deployDir'], 'backtest_results', 'runs_index.json')

        runs = []
        if os.path.exists(index_file):
            try:
                with open(index_file, 'r') as f:
                    runs = json.load(f)
            except Exception as e:
                logging.error(f'Error loading runs index: {str(e)}')

        # Most recent runs first
        runs = list(reversed(runs))

        return render_template('backtest_history.html', short_code=short_code, runs=runs)
