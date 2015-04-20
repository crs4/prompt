import unittest
from test.pymine.mining.process.discovery.heuristics import BackendTests
from pymine.mining.process.discovery.heuristics.all_connected import HeuristicMiner
import logging
logging.basicConfig(format="%(filename)s %(lineno)s %(levelname)s: %(message)s")
logger = logging.getLogger('all_connected')
# logger.setLevel(logging.DEBUG)


class TestAllConnected(BackendTests, unittest.TestCase):
    def create_miner(self, log):
        return HeuristicMiner(log)


