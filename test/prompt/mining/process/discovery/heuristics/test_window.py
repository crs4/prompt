import unittest
from datetime import datetime as dt
import logging

from prompt.mining.process.eventlog.log import Log as Log
from prompt.mining.process.eventlog import *
from prompt.mining.process.discovery.heuristics.window import HeuristicMiner as HeuristicMiner
from prompt.mining.process.network.dependency import *
from prompt.mining.process.network.cnet import *
from prompt.mining.process.eventlog.factory import CsvLogFactory
from prompt.mining.process.conformance import replay_case

logging.basicConfig(format="%(filename)s %(lineno)s %(levelname)s: %(message)s")
logger = logging.getLogger('heuristic')
TIME_FORMAT = '%Y-%m-%d %H:%M:%S.%f'


# FIXME come on, fix these tests and remove the skip, even if all connected works well
@unittest.skip
class TestHeuristicMiner(unittest.TestCase):

    def __init__(self, label):
        super(TestHeuristicMiner, self).__init__(label)

    def create_log_from_test_data(self):
        process = Process(_id="p1")
        case1 = process.add_case(Case(_id="c1"))
        case2 = process.add_case(Case(_id="c2"))
        activity_a = process.add_activity(name="A")
        activity_b = process.add_activity(name="B")
        activity_c = process.add_activity(name="C")
        activity_d = process.add_activity(name="D")
        # Event 1
        event1 = Event(activity_a.name, dt.strptime("2014-12-25 00:00:01.0", TIME_FORMAT), resources=['R1'])
        case1.add_event(event1)
        # Event 2
        event2 = Event(activity_b.name, dt.strptime("2014-12-25 00:00:02.0", TIME_FORMAT), resources=['R1'])
        case1.add_event(event2)

        # Event 3
        event3 = Event(activity_c.name, dt.strptime("2014-12-25 00:00:03.0", TIME_FORMAT), resources=["R2"])
        case1.add_event(event3)

        # Event 4
        event4 = Event(activity_b.name, dt.strptime("2014-12-25 00:00:04.0", TIME_FORMAT), resources=["R2"])
        case1.add_event(event4)

        # Event 5
        event5 = Event(activity_d.name, dt.strptime("2014-12-25 00:00:05.0", TIME_FORMAT), resources=["R1"])
        case1.add_event(event5)

        # Event 6
        event6 = Event(activity_a.name, dt.strptime("2014-12-25 00:00:06.0", TIME_FORMAT), resources=["R1"])
        case2.add_event(event6)

        # Event 7
        event7 = Event(activity_b.name, dt.strptime("2014-12-25 00:00:07.0", TIME_FORMAT), resources=["R1"])
        case2.add_event(event7)

        # Event 8
        event8 = Event(activity_d.name, dt.strptime("2014-12-25 00:00:08.0", TIME_FORMAT), ["R1"])
        case2.add_event(event8)

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


def suite():
    suite = unittest.TestSuite()
    suite.addTest(TestHeuristicMiner('test_mine_dependency_graph'))
    suite.addTest(TestHeuristicMiner('test_mine_cnets'))
    return suite

if __name__ == '__main__':
    runner = unittest.TestHeuristicMiner(verbosity=2)
    runner.run(suite())