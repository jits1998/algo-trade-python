import logging
import uuid
from core.BaseOrderManager import BaseOrderManager
from ordermgmt.Order import Order
from models.OrderStatus import OrderStatus
from models.OrderType import OrderType
from models.Direction import Direction
from utils.Utils import Utils


class BacktestOrderManager(BaseOrderManager):
    """
    Order manager for backtesting that simulates order execution
    without placing real orders
    """

    def __init__(self, short_code):
        super().__init__("backtest", None)
        self.short_code = short_code
        self.order_counter = 0

    def placeOrder(self, orderInputParams):
        """
        Simulate order placement by creating an order with a simulated order ID
        For SL orders, they are kept as OPEN until the price reaches the trigger level
        For other orders, they are filled immediately
        """
        self.order_counter += 1
        orderId = f"BACKTEST_{self.short_code}_{self.order_counter}_{uuid.uuid4().hex[:8]}"

        logging.info(
            '%s:%s:: Simulated order placed, orderId = %s, symbol = %s, qty = %d, price = %.2f, triggerPrice = %.2f, orderType = %s, tag = %s',
            self.broker, self.short_code, orderId, orderInputParams.tradingSymbol,
            orderInputParams.qty, orderInputParams.price, orderInputParams.triggerPrice,
            orderInputParams.orderType, orderInputParams.tag
        )

        order = Order(orderInputParams)
        order.orderId = orderId
        order.orderPlaceTimestamp = Utils.getExchangeTimestamp(self.short_code)
        order.lastOrderUpdateTimestamp = Utils.getExchangeTimestamp(self.short_code)

        # Store direction in order for later use
        order.direction = orderInputParams.direction

        if orderInputParams.orderType in [OrderType.SL_MARKET, OrderType.SL_LIMIT]:
            # SL orders wait for trigger price to be hit
            order.orderStatus = OrderStatus.TRIGGER_PENDING
            order.averagePrice = 0
            order.filledQty = 0
            order.pendingQty = orderInputParams.qty
            logging.info(
                '%s:%s:: SL order %s placed as TRIGGER_PENDING, waiting for trigger price %.2f to be hit',
                self.broker, self.short_code, orderId, orderInputParams.triggerPrice
            )
        elif orderInputParams.orderType == OrderType.LIMIT:
            # LIMIT orders (entry or target) wait for price to be reached
            order.orderStatus = OrderStatus.OPEN
            order.averagePrice = 0
            order.filledQty = 0
            order.pendingQty = orderInputParams.qty
            logging.info(
                '%s:%s:: LIMIT order %s placed as OPEN, waiting for price %.2f to be reached',
                self.broker, self.short_code, orderId, orderInputParams.price
            )
        else:
            # MARKET orders execute immediately at the requested price
            order.orderStatus = OrderStatus.COMPLETE
            order.averagePrice = orderInputParams.price
            order.filledQty = orderInputParams.qty
            order.pendingQty = 0

        return order

    def modifyOrder(self, order, orderModifyParams, tradeQty):
        """
        Simulate order modification
        """
        if order is None:
            logging.warning('%s:%s:: Cannot modify None order',
                            self.broker, self.short_code)
            return order

        logging.info(
            '%s:%s:: Simulated order modification for orderId = %s, newPrice = %.2f, newTriggerPrice = %.2f',
            self.broker, self.short_code, order.orderId,
            orderModifyParams.newPrice if orderModifyParams.newPrice > 0 else 0,
            orderModifyParams.newTriggerPrice if orderModifyParams.newTriggerPrice > 0 else 0
        )

        # Update order parameters
        if orderModifyParams.newPrice > 0:
            order.price = orderModifyParams.newPrice
            order.averagePrice = orderModifyParams.newPrice

        if orderModifyParams.newTriggerPrice > 0:
            order.triggerPrice = orderModifyParams.newTriggerPrice

        if orderModifyParams.newQty > 0:
            order.qty = orderModifyParams.newQty
            order.filledQty = orderModifyParams.newQty
            order.pendingQty = 0

        if orderModifyParams.newOrderType is not None:
            order.orderType = orderModifyParams.newOrderType

        order.lastOrderUpdateTimestamp = Utils.getExchangeTimestamp(self.short_code)

        return order

    def modifyOrderToMarket(self, order):
        """
        Simulate modification of order to market order
        """
        if order is None:
            logging.warning(
                '%s:%s:: Cannot modify None order to market', self.broker, self.short_code)
            return order

        logging.info(
            '%s:%s:: Simulated order modification to MARKET for orderId = %s',
            self.broker, self.short_code, order.orderId
        )

        from models.OrderType import OrderType
        order.orderType = OrderType.MARKET
        order.lastOrderUpdateTimestamp = Utils.getExchangeTimestamp(self.short_code)

        return order

    def cancelOrder(self, order):
        """
        Simulate order cancellation
        """
        if order is None:
            logging.warning('%s:%s:: Cannot cancel None order',
                            self.broker, self.short_code)
            return

        logging.info(
            '%s:%s:: Simulated order cancellation for orderId = %s',
            self.broker, self.short_code, order.orderId
        )

        order.orderStatus = OrderStatus.CANCELLED
        order.lastOrderUpdateTimestamp = Utils.getExchangeTimestamp(self.short_code)

    def updateOrder(self, order, data):
        """
        Simulate order update from broker feed
        In backtest mode, orders are already updated immediately on placement
        """
        if order is None:
            return

        # In backtest mode, we don't need to do anything here
        # Orders are already marked as complete when placed
        pass

    def fetchAndUpdateAllOrderDetails(self, orders):
        """
        Simulate fetching and updating all order details
        In backtest mode, all orders are already up-to-date
        """
        # Return empty list as there are no missing orders in backtest
        return []

    def checkAndExecuteSLOrders(self, trades, candle_data_map):
        """
        Check active trades and execute their pending orders if price conditions are met
        This includes SL orders, entry orders, and target orders

        Args:
            trades: List of all trades
            candle_data_map: Dict mapping tradingSymbol -> candle data with 'open', 'high', 'low', 'close'

        Logic for different order types:
        - SL orders (SHORT exit): trigger when price goes UP (high >= triggerPrice)
        - SL orders (LONG exit): trigger when price goes DOWN (low <= triggerPrice)
        - SL_LIMIT entry (SHORT): trigger when price goes UP (high >= triggerPrice) — re-entry waits for price to recover
        - SL_LIMIT entry (LONG): trigger when price goes DOWN (low <= triggerPrice) — not currently used
        - LIMIT entry (BUY): fills when price goes DOWN (low <= limitPrice)
        - LIMIT entry (SELL): fills when price goes UP (high >= limitPrice)
        - LIMIT target (BUY to close SHORT): fills when price goes DOWN (low <= limitPrice)
        - LIMIT target (SELL to close LONG): fills when price goes UP (high >= limitPrice)
        """
        from trademgmt.TradeState import TradeState

        for trade in trades:
            # Check all trades (not just ACTIVE) as CREATED trades might have pending entry orders
            if trade.tradeState not in [TradeState.CREATED, TradeState.ACTIVE]:
                continue

            # Check if we have candle data for this symbol
            if trade.tradingSymbol not in candle_data_map:
                continue

            candle_data = candle_data_map[trade.tradingSymbol]

            # Check entry orders (for CREATED trades waiting to be triggered)
            for entryOrder in trade.entryOrder:
                if entryOrder.orderStatus not in [OrderStatus.OPEN, OrderStatus.TRIGGER_PENDING]:
                    continue

                filled = False
                execution_price = entryOrder.price

                # For SL orders, check if trigger price was hit
                if entryOrder.orderType in [OrderType.SL_MARKET, OrderType.SL_LIMIT]:
                    # Entry SL orders work like regular SL orders - trigger on price movement
                    if trade.direction == Direction.LONG:
                        # Buying: entry SL triggers if price goes DOWN to trigger price
                        if candle_data['low'] <= entryOrder.triggerPrice:
                            filled = True
                            # If candle opened below the trigger, fill at open (already past trigger)
                            if candle_data['open'] <= entryOrder.triggerPrice:
                                execution_price = candle_data['open']
                            else:
                                execution_price = entryOrder.triggerPrice
                            logging.info(
                                '%s:%s:: Entry SL order %s TRIGGERED for LONG on %s - candle low %.2f <= trigger %.2f',
                                self.broker, self.short_code, entryOrder.orderId,
                                trade.tradingSymbol, candle_data['low'], entryOrder.triggerPrice
                            )
                    else:  # SHORT
                        # Selling short: entry SL triggers if price goes DOWN to trigger price
                        # (re-entry waits for option price to fall back to the original entry level)
                        if candle_data['low'] <= entryOrder.triggerPrice:
                            filled = True
                            # If candle opened below the trigger, fill at open (already past trigger)
                            if candle_data['open'] <= entryOrder.triggerPrice:
                                execution_price = candle_data['open']
                            else:
                                execution_price = entryOrder.triggerPrice
                            logging.info(
                                '%s:%s:: Entry SL order %s TRIGGERED for SHORT on %s - candle low %.2f <= trigger %.2f',
                                self.broker, self.short_code, entryOrder.orderId,
                                trade.tradingSymbol, candle_data['low'], entryOrder.triggerPrice
                            )
                # For LIMIT orders, check if price reached the limit
                elif entryOrder.orderType == OrderType.LIMIT:
                    if trade.direction == Direction.LONG:
                        # Buying: fills if price goes down to or below limit price
                        if candle_data['low'] <= entryOrder.price:
                            filled = True
                            execution_price = entryOrder.price
                    else:  # SHORT
                        # Selling: fills if price goes up to or above limit price
                        if candle_data['high'] >= entryOrder.price:
                            filled = True
                            execution_price = entryOrder.price

                if filled:
                    entryOrder.orderStatus = OrderStatus.COMPLETE
                    entryOrder.averagePrice = execution_price
                    entryOrder.filledQty = entryOrder.qty
                    entryOrder.pendingQty = 0
                    entryOrder.lastOrderUpdateTimestamp = Utils.getExchangeTimestamp(self.short_code)

                    # Update trade.entry and trade.filledQty when entry order fills
                    # This is crucial for CREATED trades with SL entry orders
                    if trade.filledQty == 0:
                        # First fill
                        trade.entry = execution_price
                        trade.filledQty = entryOrder.filledQty
                    else:
                        # Multiple entry orders - calculate weighted average
                        trade.entry = (trade.entry * trade.filledQty + execution_price * entryOrder.filledQty) / (trade.filledQty + entryOrder.filledQty)
                        trade.filledQty += entryOrder.filledQty

                    logging.info(
                        '%s:%s:: Entry order %s FILLED at price %.2f for %s, qty = %d, trade.entry updated to %.2f',
                        self.broker, self.short_code, entryOrder.orderId,
                        execution_price, trade.tradingSymbol, entryOrder.filledQty, trade.entry
                    )

            # Check SL orders (for ACTIVE trades)
            for slOrder in trade.slOrder:
                if slOrder.orderStatus != OrderStatus.TRIGGER_PENDING:
                    continue

                triggered = False
                execution_price = slOrder.triggerPrice

                # For SHORT positions: you sold, so SL is above (exit when price goes UP)
                # For LONG positions: you bought, so SL is below (exit when price goes DOWN)
                if trade.direction == Direction.SHORT:
                    # Sell position: SL triggers if price goes UP to trigger price
                    if candle_data['high'] >= slOrder.triggerPrice:
                        triggered = True
                        logging.info(
                            '%s:%s:: SL order %s TRIGGERED for SHORT position on %s - candle high %.2f >= trigger %.2f',
                            self.broker, self.short_code, slOrder.orderId,
                            trade.tradingSymbol, candle_data['high'], slOrder.triggerPrice
                        )

                elif trade.direction == Direction.LONG:
                    # Buy position: SL triggers if price goes DOWN to trigger price
                    if candle_data['low'] <= slOrder.triggerPrice:
                        triggered = True
                        logging.info(
                            '%s:%s:: SL order %s TRIGGERED for LONG position on %s - candle low %.2f <= trigger %.2f',
                            self.broker, self.short_code, slOrder.orderId,
                            trade.tradingSymbol, candle_data['low'], slOrder.triggerPrice
                        )

                if triggered:
                    # Execute the SL at the limit price, capped by what the candle actually reached.
                    # SHORT SL: limit price is above trigger, cap at candle high.
                    # LONG SL: limit price is below trigger, cap at candle low.
                    if trade.direction == Direction.SHORT:
                        execution_price = min(slOrder.price, candle_data['high'])
                    else:
                        execution_price = max(slOrder.price, candle_data['low'])
                    slOrder.orderStatus = OrderStatus.COMPLETE
                    slOrder.averagePrice = execution_price
                    slOrder.filledQty = slOrder.qty
                    slOrder.pendingQty = 0
                    slOrder.lastOrderUpdateTimestamp = Utils.getExchangeTimestamp(self.short_code)
                    logging.info(
                        '%s:%s:: SL order %s EXECUTED at %.2f (trigger was %.2f, limit was %.2f) for %s, qty = %d',
                        self.broker, self.short_code, slOrder.orderId,
                        execution_price, slOrder.triggerPrice, slOrder.price, trade.tradingSymbol, slOrder.qty
                    )

            # Check target orders (for ACTIVE trades with exit orders)
            for targetOrder in trade.targetOrder:
                if targetOrder.orderStatus != OrderStatus.OPEN:
                    continue

                filled = False
                execution_price = targetOrder.price

                # Target orders are the opposite direction of the trade
                # If trade is LONG, target is SELL (close by selling)
                # If trade is SHORT, target is BUY (close by buying)
                if targetOrder.orderType == OrderType.LIMIT:
                    if trade.direction == Direction.LONG:
                        # Closing LONG position (selling): fills if price goes up to or above target
                        if candle_data['high'] >= targetOrder.price:
                            filled = True
                            execution_price = targetOrder.price
                    else:  # SHORT
                        # Closing SHORT position (buying): fills if price goes down to or below target
                        if candle_data['low'] <= targetOrder.price:
                            filled = True
                            execution_price = targetOrder.price

                if filled:
                    targetOrder.orderStatus = OrderStatus.COMPLETE
                    targetOrder.averagePrice = execution_price
                    targetOrder.filledQty = targetOrder.qty
                    targetOrder.pendingQty = 0
                    targetOrder.lastOrderUpdateTimestamp = Utils.getExchangeTimestamp(self.short_code)
                    logging.info(
                        '%s:%s:: Target LIMIT order %s FILLED at price %.2f for %s, qty = %d',
                        self.broker, self.short_code, targetOrder.orderId,
                        execution_price, trade.tradingSymbol, targetOrder.qty
                    )
