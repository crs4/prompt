import unittest
from test.pymine.mining.process.discovery.heuristics import BackendTests
from pymine.mining.process.discovery.heuristics.all_connected import HeuristicMiner
import logging
logging.basicConfig(format="%(filename)s %(lineno)s %(levelname)s: %(message)s")
logger = logging.getLogger('all_connected')
# logger.setLevel(logging.DEBUG)


from pymine.mining.process.eventlog.factory import create_process_log_from_list, create_log_from_file


def get_binding_set(binding):
    return {frozenset(bi.node_set) for bi in binding}


class TestAllConnected(BackendTests, unittest.TestCase):
    def create_miner(self, log):
        return HeuristicMiner(log)