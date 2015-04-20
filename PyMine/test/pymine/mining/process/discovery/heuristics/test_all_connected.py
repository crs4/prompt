import unittest
from test.pymine.mining.process.discovery.heuristics import BackendTests
from pymine.mining.process.discovery.heuristics.all_connected import HeuristicMiner


class TestAllConnected(BackendTests, unittest.TestCase):
    def create_miner(self, log):
        return HeuristicMiner(log)


