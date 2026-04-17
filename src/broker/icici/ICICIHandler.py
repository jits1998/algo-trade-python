import csv
import datetime
from io import BytesIO, TextIOWrapper
from urllib.request import urlopen, urlretrieve
from zipfile import ZipFile

from breeze_connect import config as breeze_config

from core.BrokerHandler import BrokerHandler
from models.OrderStatus import OrderStatus
from models.OrderType import OrderType


class ICICIHandler(BrokerHandler):

    def __init__(self, broker, config):
        self.broker = broker
        self.config = config
        self.symbol_to_stock_code = {}

    def getStockCode(self, symbol):
        return self.symbol_to_stock_code.get(symbol, symbol)

    def set_access_token(self, access_token):
        self.broker.generate_session(session_token=access_token, api_secret=self.config.appSecret)

    def margins(self):
        raise Exception("Method not to be called")

    def positions(self):
        raise Exception("Method not to be called")

    def orders(self):
        order_list = self.broker.get_order_list(
            exchange_code="NFO",
            from_date=datetime.datetime.now().isoformat()[:10] + "T05:30:00.000Z",
            to_date=datetime.datetime.now().isoformat()[:10] + "T05:30:00.000Z",
        )["Success"]

        if order_list == None:
            return []
        else:
            for order in order_list:
                self.updateOrderType(order)
                self.updateOrderStatus(order)
                order["tradingsymbol"] = [
                    x
                    for x in self.instruments
                    if x["name"] == order["stock_code"]
                    and x["strike"] == str(order["strike_price"]).split(".")[0]
                    and x["expiry"] == order["expiry_date"]
                    and x["instrument_type"] == ("PE" if order["right"] == "Put" else "CE")
                ][0]["tradingsymbol"]
                order["tag"] = order["user_remark"]
                order["transaction_type"] = order["action"]
        return order_list

    def quote(self, isd):
        product_type = ""
        right = ""
        if isd["instrument_type"] == "PE":
            product_type = "options"
            right = "PUT"
        elif isd["instrument_type"] == "CE":
            product_type = "options"
            right = "CALL"
        elif isd["expiry"] != "":
            product_type = "futures"
            right = "Others"

        return self.broker.get_quotes(
            stock_code=isd["name"],
            exchange_code=isd["exchange"],
            expiry_date=isd["expiry"],
            product_type=product_type,
            right=right,
            strike_price=isd["strike"],
        )["Success"][0]

    def instruments(self, exchange):
        # get zerodha instruments file to get tradesymbols
        tradingSymbolDict = {}
        f, r = urlretrieve("https://api.kite.trade/instruments")
        with open(f, newline="") as csvfile:
            r = csv.DictReader(csvfile)
            for row in r:
                tradingSymbolDict[row["exchange_token"]] = {
                    "tradingsymbol": row["tradingsymbol"],
                    "name": row["name"],
                }

        resp = urlopen(breeze_config.SECURITY_MASTER_URL)
        zipfile = ZipFile(BytesIO(resp.read()))
        mapper_exchangecode_to_file = breeze_config.ISEC_NSE_CODE_MAP_FILE

        file_key = mapper_exchangecode_to_file.get(exchange.lower())
        if file_key is None:
            return []
        required_file = zipfile.open(file_key)

        if exchange == "NFO":
            exchange = "fonse"
        elif exchange == "BFO":
            exchange = "bfonse"

        # field_names={'nse':['Token', 'ShortName', 'Series', 'CompanyName', 'ticksize', 'Lotsize',
        #              'DateOfListing', 'DateOfDeListing', 'IssuePrice', 'FaceValue', 'ISINCode',
        #              '52WeeksHigh', '52WeeksLow', 'LifeTimeHigh', 'LifeTimeLow', 'HighDate',
        #              'LowDate', 'Symbol', 'InstrumentType', 'PermittedToTrade', 'IssueCapital',
        #              'WarningPercent', 'FreezePercent', 'CreditRating', 'IssueRate', 'IssueStartDate',
        #             'InterestPaymentDate', 'IssueMaturityDate', 'BoardLotQty', 'Name', 'ListingDate',
        #             'ExpulsionDate', 'ReAdmissionDate', 'RecordDate', 'ExpiryDate', 'NoDeliveryStartDate',
        #             'NoDeliveryEndDate', 'MFill', 'AON', 'ParticipantInMarketIndex', 'BookClsStartDate',
        #             'NoDeliveryEndDate', 'MFill', 'AON', 'ParticipantInMarketIndex', 'BookClsStartDate',
        #             'BookClsEndDate', 'EGM', 'AGM', 'Interest', 'Bonus', 'Rights', 'Dividends',
        #             'LocalUpdateDateTime', 'DeleteFlag', 'Remarks', 'NormalMarketStatus', 'OddLotMarketStatus',
        #             'SpotMarketStatus', 'AuctionMarketStatus', 'NormalMarketEligibility', 'OddLotlMarketEligibility',
        #             'SpotMarketEligibility', 'AuctionlMarketEligibility', 'MarginPercentage', 'ExchangeCode'],
        #             'bse':None,
        #             'fonse': None}
        reader = csv.DictReader(TextIOWrapper(required_file, "utf-8"))
        records = []

        for row in reader:
            instrument = {}
            if reader.line_num == 1:
                continue

            # row["last_price"] = float(row["last_price"])
            # row["strike"] = float(row["strike"])
            # row["tick_size"] = float(row["tick_size"])
            # row["lot_size"] = int(row["lot_size"])

            if exchange.lower() == "nse" and row[' "Series"'] in ["EQ", "0"]:
                is_index = row[' "Series"'] == "0"
                instrument["exchange_token"] = row["Token"]
                instrument["tradingsymbol"] = (
                    row[' "ExchangeCode"'] if not is_index else row["Token"]
                )
                instrument["instrument_token"] = row["Token"]
                instrument["name"] = row[' "ShortName"']
                instrument["last_price"] = 0.0
                instrument["expiry"] = row[' "ExpiryDate"']
                instrument["strike"] = 0
                instrument["tick_size"] = float(row[' "ticksize"'])
                instrument["lot_size"] = int(row[' "Lotsize"'])
                instrument["instrument_type"] = "EQ"
                instrument["segment"] = "NSE" if not is_index else "INDICES"
                instrument["exchange"] = "NSE"
                # Map tradingsymbol to ICICI stock code
                if is_index:
                    # Index: e.g. "NIFTY BANK" -> "CNXBAN"
                    if row["Token"] != row[' "ShortName"']:
                        self.symbol_to_stock_code[row["Token"]] = row[' "ShortName"']
                else:
                    # Equity: e.g. "ICICIBANK" -> "ICIBAN"
                    if row[' "ExchangeCode"'] != row[' "ShortName"']:
                        self.symbol_to_stock_code[row[' "ExchangeCode"']] = row[' "ShortName"']
                records.append(instrument)
            elif exchange.lower() == "bse":
                # Map BSE tradingsymbol to ICICI stock code (e.g. "SENSEX" -> "BSESEN")
                tradingsymbol = row["ExchangeCode"] if row["ExchangeCode"] else row["Token"]
                if tradingsymbol != row["ShortName"]:
                    self.symbol_to_stock_code[tradingsymbol] = row["ShortName"]
                instrument["exchange_token"] = row["Token"]
                instrument["tradingsymbol"] = tradingsymbol
                instrument["instrument_token"] = row["Token"]
                instrument["name"] = row["ShortName"]
                instrument["last_price"] = 0.0
                instrument["expiry"] = ""
                instrument["strike"] = 0
                instrument["tick_size"] = float(row["TickSize"]) if row["TickSize"] != "0" else 0.0
                instrument["lot_size"] = int(row["LotSize"])
                instrument["instrument_type"] = "EQ"
                instrument["segment"] = "BSE"
                instrument["exchange"] = "BSE"
                records.append(instrument)
            elif exchange.lower() in ("fonse", "bfonse"):
                is_bfo = exchange.lower() == "bfonse"
                zerodha = tradingSymbolDict.get(row["Token"], None)
                if zerodha is None:
                    continue
                if zerodha["name"] != row["ShortName"]:
                    self.symbol_to_stock_code[zerodha["name"]] = row["ShortName"]
                instrument["exchange_token"] = row["Token"]
                instrument["tradingsymbol"] = zerodha["tradingsymbol"]
                instrument["instrument_token"] = row["Token"]
                instrument["name"] = row["ShortName"]
                instrument["last_price"] = 0.0
                instrument["expiry"] = row["ExpiryDate"]
                instrument["strike"] = row["StrikePrice"]
                instrument["tick_size"] = float(row["TickSize"])
                instrument["lot_size"] = int(row["LotSize"])
                instrument["instrument_type"] = (
                    row["OptionType"] if row["OptionType"] != "XX" else "FUT"
                )
                instrument["segment"] = ("BFO" if is_bfo else "NFO") + "-" + row["Series"][:3]
                instrument["exchange"] = "BFO" if is_bfo else "NFO"
                records.append(instrument)
            else:
                pass

            # Parse date
            # if len(instrument["expiry"]) == 10:
            #   instrument["expiry"] = dateutil.parser.parse(instrument["expiry"]).date()

        return records

    def updateOrderStatus(self, order):
        if order["status"] == "Executed":
            order["status"] = OrderStatus.COMPLETE
        elif order["status"] == "Ordered":
            if order["order_type"] == OrderType.LIMIT:
                order["status"] = OrderStatus.OPEN
            else:
                order["status"] = OrderStatus.TRIGGER_PENDING
        else:
            order["status"] = order["status"].upper()

    def updateOrderType(self, order):
        if order["order_type"] == "Limit":
            order["order_type"] = OrderType.LIMIT
        elif order["order_type"] == "Market":
            order["order_type"] = OrderType.MARKET
        elif order["order_type"] == "StopLoss":
            order["order_type"] = OrderType.SL_LIMIT
        else:
            order["order_type"] = OrderType.SL_MARKET

    def get_historical_data(
        self,
        interval,
        from_date,
        to_date,
        stock_code,
        exchange_code,
        product_type,
        expiry_date="",
        right="others",
        strike_price="",
    ):
        """
        Fetch historical data from Breeze API.

        Args:
            interval: '1minute', '5minute', '30minute', '1day'
            from_date: ISO 8601 format (e.g., '2024-01-30T09:15:00.000Z')
            to_date: ISO 8601 format (e.g., '2024-01-30T15:30:00.000Z')
            stock_code: Stock/Index name (e.g., 'NIFTY', 'BANKNIFTY')
            exchange_code: 'NSE', 'NFO', 'BSE', 'BFO'
            product_type: 'cash', 'futures', 'options'
            expiry_date: Expiry date in ISO format (optional for cash)
            right: 'call', 'put', 'others' (optional for cash)
            strike_price: Strike price as string (optional for cash)

        Returns:
            Response dictionary with Status and Success/Error keys
        """
        return self.broker.get_historical_data_v2(
            interval=interval,
            from_date=from_date,
            to_date=to_date,
            stock_code=stock_code,
            exchange_code=exchange_code,
            product_type=product_type,
            expiry_date=expiry_date,
            right=right,
            strike_price=strike_price,
        )
