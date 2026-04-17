from flask.views import MethodView
from flask import render_template, jsonify
from flask import redirect
from utils.Utils import Utils
from database import get_db_engine

import json
import plotly
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from datetime import datetime, date
from itertools import cycle
import plotly.express as px


class ChartAPI(MethodView):
  def get(self, short_code, data=False):
    if Utils.getTradeManager(short_code) is None:
      if data:
        return jsonify({"error": "TradeManager not found"}), 404
      return redirect("/me/" + short_code, code=302)

    try:
      graphJSON = self._build_graph_json(short_code)
    except Exception as err:
      if data:
        return jsonify({"error": str(err)}), 500
      return "Error while connecting to QuestDB " + str(err)

    if graphJSON is None:
      if data:
        return jsonify({"error": "Data Not found"}), 404
      return "Data Not found"

    if data:
      return graphJSON, 200, {"Content-Type": "application/json"}

    return render_template("chart.html", short_code=short_code)

  def _build_graph_json(self, short_code):
    engine = get_db_engine()
    df = pd.read_sql_query(
      "select * from %(short_code)s where ts > to_timestamp(%(dstart)s, 'yyyy-MM-dd HH:mm:ss')",
      con=engine,
      params={"short_code": short_code, "dstart": date.today().strftime("%Y-%m-%d %H:%M:%S")},
    )

    if df.empty:
      return None

    # Parse ts and convert to datetime type
    df["ts"] = pd.to_datetime(df["ts"]).dt.strftime('%Y-%m-%d %H:%M:%S')
    df["ts"] = pd.to_datetime(df["ts"])

    # Group pnl in 1min frequency and select last pnl for each trade
    resample_data = df.groupby([pd.Grouper(key='ts', freq='1min'), "tradeId", "strategy"]).agg(pnl=('pnl', 'last')).reset_index()

    # Create pivot table; forward fill pnl till end if trade is closed
    p_table = pd.pivot_table(resample_data, values=["pnl"], index=['strategy', 'tradeId'],
                  columns=['ts'], margins=True, margins_name='total_pnl').ffill(axis=1).replace(np.nan, 0)
    p_table.columns = p_table.columns.droplevel(0)

    # Fill missing timestamps from 9:30 to last known timestamp
    p_table = p_table.reindex(
      columns=pd.date_range(
        datetime.now().replace(minute=30, hour=9, second=0).strftime('%Y-%m-%d %H:%M:%S'),
        df.iloc[-1]["ts"].strftime('%Y-%m-%d %H:%M:%S'),
        freq="1min",
      )
    ).ffill(axis=1).replace(np.nan, 0)

    # Group by strategy and sum pnl at every timestamp
    g_table = p_table.groupby(["strategy"]).sum()
    g_table.loc["total_pnl"] = g_table[:-1].sum()

    x_axis = g_table.columns.to_list()[:-1]
    colors = cycle(px.colors.qualitative.Vivid)
    fig = go.Figure()

    for s_name in g_table.index:
      if s_name != "total_pnl":
        fig.add_trace(go.Bar(name=s_name, x=x_axis, y=g_table.loc[s_name].tolist(), marker_color=next(colors)))

    fig.update_layout(barmode='group', xaxis_range=[
      datetime.now().replace(hour=9, minute=30, second=0),
      datetime.now().replace(hour=15, minute=30, second=0),
    ])

    total_y = g_table.loc["total_pnl"].tolist()
    total_pnl_series = pd.Series(total_y)
    fig.add_scattergl(name="Total PNL", x=x_axis, y=total_y, line={'color': 'black'})
    fig.add_scattergl(name="Total PNL (+)", x=x_axis, y=total_pnl_series.clip(lower=0).tolist(),
                      fill='tozeroy', fillcolor='rgba(0,180,0,0.15)', line={'color': 'rgba(0,0,0,0)'}, showlegend=False)
    fig.add_scattergl(name="Total PNL (-)", x=x_axis, y=total_pnl_series.clip(upper=0).tolist(),
                      fill='tozeroy', fillcolor='rgba(220,0,0,0.15)', line={'color': 'rgba(0,0,0,0)'}, showlegend=False)

    return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)

