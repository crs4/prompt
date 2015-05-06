import unittest
from test.pymine.mining.process.discovery.heuristics import BackendTests
from pymine.mining.process.discovery.heuristics.all_connected import HeuristicMiner
import os
import logging
logging.basicConfig(format="%(filename)s %(lineno)s %(levelname)s: %(message)s")
logger = logging.getLogger('all_connected')


def get_binding_set(binding):
    return {frozenset(bi.node_set) for bi in binding}


@unittest.skipUnless("HADOOP_HOME" in os.environ, "HADOOP_HOME not set")
class TestAllConnectedMR(BackendTests, unittest.TestCase):

    def create_miner(self, log):
        from pymine.mining.process.discovery.heuristics.mapred.dependency_mr import DependencyMiner
        from pymine.mining.process.discovery.heuristics.mapred.binding_miner_mr import BindingMiner

        return HeuristicMiner(log, dep_miner_cls=DependencyMiner, b_miner_cls=BindingMiner)


if __name__ == '__main__':
    unittest.main()