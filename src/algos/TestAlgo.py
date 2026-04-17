from core.BaseAlgo import BaseAlgo
from strategies.ManualStrategy import ManualStrategy
from strategies.hedge.N1ReHedge import N1ReHedge
from strategies.hedge.S3RsHedge import S3RsHedge

class AlgoTypeA(BaseAlgo):

    def startStrategies(self, short_code, multiple = 0):
    # start running strategies: Run each strategy in a separate thread
    # run = [expiry, mon, tue, wed, thru, fri, -4expiry, -3 expiry, -2 expiry, -1 expiry]

        self.startStrategy(
            N1ReHedge, short_code, multiple, tradeManager, r([4, 0, 0, 0, 0, 0, 0, 0, 0, 0], ns)
        )

        self.startStrategy(
            S3RsHedge, short_code, multiple, tradeManager, r([4, 0, 0, 0, 0, 0, 0, 0, 0, 0], ss)
        )

        # Manual strategy for custom entries
        self.startStrategy(
            ManualStrategy, short_code, multiple, tradeManager, [1, 1, 1, 1, 1, 1, 1, 1, 1, 1]
        )

