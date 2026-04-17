from flask.views import MethodView
from flask import request, redirect, session

from core.Controller import Controller 

class BrokerLoginAPI(MethodView):
  methods = ['GET', 'POST']
  def get(self, broker):
    # Store redirect_to parameter in session if provided
    redirect_to = request.args.get('redirect_to', None)
    if redirect_to:
      session['redirect_to'] = redirect_to

    redirectUrl = Controller.handleBrokerLogin(request.args, session['short_code'])
    if Controller.getBrokerLogin(session['short_code']).getAccessToken() is not None:
      session['access_token'] = Controller.getBrokerLogin(session['short_code']).accessToken

      # Override redirect URL if redirect_to is set in session
      if session.get('redirect_to') == 'backtest':
        redirectUrl = f"/backtesting/{session['short_code']}?loggedIn=true"
        session.pop('redirect_to', None)  # Clear the redirect_to after using it

    return redirect(redirectUrl, code=302)
  
  def post(self, broker):
    if broker == 'icici':
      return redirect(request.url, code=302)
    return None