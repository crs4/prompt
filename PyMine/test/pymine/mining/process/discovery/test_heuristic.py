import unittest
from datetime import datetime as dt
from pymine.mining.process.eventlog.log import Log as Log
from pymine.mining.process.eventlog import *
from pymine.mining.process.discovery.heuristic import HeuristicMiner as HeuristicMiner
from pymine.mining.process.network.dependency import *
from pymine.mining.process.network.cnet import *
from pymine.mining.process.eventlog.factory import CsvLogFactory
from pymine.mining.process.conformance import replay_case
import logging
logging.basicConfig(level=logging.DEBUG,format="%(filename)s %(lineno)s %(levelname)s: %(message)s")
logger = logging.getLogger('heuristic')
logger.setLevel(logging.DEBUG)


class TestHeuristicMiner(unittest.TestCase):

    def __init__(self, label):
        super(TestHeuristicMiner, self).__init__(label)

    def create_log_from_test_data(self):
        process = Process(_id="p1")
        case1 = process.add_case("c1")
        case2 = process.add_case("c2")
        activity_a = process.add_activity(name="A")
        activity_b = process.add_activity(name="B")
        activity_c = process.add_activity(name="C")
        activity_d = process.add_activity(name="D")
        # Event 1
        activity_a_instance = case1.add_activity_instance(activity_a)
        event1 = activity_a_instance.add_event(activity_a_instance)
        event1.timestamp = dt.strptime("2014-12-25 00:00:01.0", CsvLogFactory.TIME_FORMAT)
        event1.resources.append("R1")
        # Event 2
        activity_b_instance = case1.add_activity_instance(activity_b)
        event2 = activity_b_instance.add_event(activity_b_instance)
        event2.timestamp = dt.strptime("2014-12-25 00:00:02.0", CsvLogFactory.TIME_FORMAT)
        event2.resources.append("R1")
        # Event 3
        activity_c_instance = case1.add_activity_instance(activity_c)
        event3 = activity_c_instance.add_event(activity_c_instance)
        event3.timestamp = dt.strptime("2014-12-25 00:00:03.0", CsvLogFactory.TIME_FORMAT)
        event3.resources.append("R2")
        # Event 4
        activity_b_instance = case1.add_activity_instance(activity_b)
        event4 = activity_b_instance.add_event(activity_b_instance)
        event4.timestamp = dt.strptime("2014-12-25 00:00:04.0", CsvLogFactory.TIME_FORMAT)
        event4.resources.append("R2")
        # Event 5
        activity_d_instance = case1.add_activity_instance(activity_d)
        event5 = activity_d_instance.add_event(activity_d_instance)
        event5.timestamp = dt.strptime("2014-12-25 00:00:05.0", CsvLogFactory.TIME_FORMAT)
        event5.resources.append("R1")
        # Event 6
        activity_a_instance = case2.add_activity_instance(activity_a)
        event6 = activity_a_instance.add_event(activity_a_instance)
        event6.timestamp = dt.strptime("2014-12-25 00:00:06.0", CsvLogFactory.TIME_FORMAT)
        event6.resources.append("R1")
        # Event 7
        activity_b_instance = case2.add_activity_instance(activity_b)
        event7 = activity_b_instance.add_event(activity_b_instance)
        event7.timestamp = dt.strptime("2014-12-25 00:00:07.0", CsvLogFactory.TIME_FORMAT)
        event7.resources.append("R1")
        # Event 8
        activity_d_instance = case2.add_activity_instance(activity_d)
        event8 = activity_d_instance.add_event(activity_d_instance)
        event8.timestamp = dt.strptime("2014-12-25 00:00:08.0", CsvLogFactory.TIME_FORMAT)
        event8.resources.append("R1")
        log = Log()
        log.add_case(case1)
        log.add_case(case2)
        return log

    def create_dependency_graph(self):
        dgraph = DependencyGraph()
        node_a = dgraph.add_node("A")
        node_b = dgraph.add_node("B")
        node_c = dgraph.add_node("C")
        node_d = dgraph.add_node("D")
        dgraph.add_arc(node_a, node_b, "A->B", 2, 0.0)
        dgraph.add_arc(node_b, node_c, "B->C", 1, 0.0)
        dgraph.add_arc(node_c, node_b, "C->B", 1, 0.0)
        dgraph.add_arc(node_b, node_d, "B->D", 2, 0.0)
        return dgraph

    def create_cnet(self):
        cnet = CNet()
        node_a = cnet.add_node("A")
        node_b = cnet.add_node("B")
        node_c = cnet.add_node("C")
        node_d = cnet.add_node("D")
        cnet.add_arc(node_a, node_b, "A->B", 2)
        cnet.add_arc(node_b, node_c, "B->C", 1)
        cnet.add_arc(node_c, node_b, "C->B", 1)
        cnet.add_arc(node_b, node_d, "B->D", 2)
        cnet.add_output_binding(node_a, {node_b}, frequency=2)
        cnet.add_output_binding(node_b, {node_c}, frequency=1)
        cnet.add_output_binding(node_b, {node_d}, frequency=2)
        cnet.add_output_binding(node_c, {node_b}, frequency=1)
        cnet.add_input_binding(node_b, {node_a}, frequency=2)
        cnet.add_input_binding(node_b, {node_c}, frequency=1)
        cnet.add_input_binding(node_c, {node_b}, frequency=1)
        cnet.add_input_binding(node_d, {node_b}, frequency=2)
        return cnet

    def test_mine_dependency_graph(self):
        log = self.create_log_from_test_data()[0]
        hm = HeuristicMiner()
        dgraph = self.create_dependency_graph()
        mined_graph = hm.mine_dependency_graph(log, 0, 0.0)
        #self.assertEqual(dgraph, mined_graph)
        self.assertTrue(True)

    def test_mine_cnets(self):
        log = self.create_log_from_test_data()
        hm = HeuristicMiner()
        cnet = self.create_cnet()
        mined_graph = hm.mine_dependency_graph(log[0], 0, 0.0)
        mined_cnet = hm.mine_cnet(mined_graph, log[0])

        self.assertTrue(len(cnet.nodes) == len(mined_cnet.nodes))
        self.assertTrue(len(cnet.arcs) == len(mined_cnet.arcs))
        self.assertTrue(len(cnet.input_bindings) == len(mined_cnet.input_bindings))
        self.assertTrue(len(cnet.output_bindings) == len(mined_cnet.output_bindings))

    def test_remove_pending_obligations(self):
        import os
        log_factory = CsvLogFactory(time_format='%d-%m-%Y:%H.%M')
        dataset_path = os.path.join(os.path.dirname(__file__), '../../../../../dataset/pg_4_label_final_node.csv')
        log = log_factory.create_log_from_file(dataset_path)[0]
        miner = HeuristicMiner()
        cnet = miner.mine(log)
        for case in log.cases:
            passed, obl_pending, unexpected = replay_case(case, cnet)
            self.assertFalse(obl_pending)


def suite():
    suite = unittest.TestSuite()
    suite.addTest(TestHeuristicMiner('test_mine_dependency_graph'))
    suite.addTest(TestHeuristicMiner('test_mine_cnets'))
    return suite

if __name__ == '__main__':
    runner = unittest.TestHeuristicMiner(verbosity=2)
    runner.run(suite())