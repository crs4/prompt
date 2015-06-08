import unittest
from test.pymine.mining.process.discovery.heuristics import BackendTests
from pymine.mining.process.discovery.heuristics.all_connected import HeuristicMiner
import logging
import os
from pymine.mining.process.eventlog.factory import create_log_from_file, create_process_log_from_list
logging.basicConfig(format="%(filename)s %(lineno)s %(levelname)s: %(message)s")
logger = logging.getLogger('all_connected')


def get_binding_set(binding):
    return {frozenset(bi.node_set) for bi in binding}


class TestAllConnected(BackendTests, unittest.TestCase):
    def create_miner(self, log):
        return HeuristicMiner(log)


if __name__ == '__main__':
    unittest.main()