import unittest
from pymine.mining.process.eventlog.log import Log as Log
from pymine.mining.process.eventlog import *
from pymine.mining.process.discovery.heuristic import HeuristicMiner as HeuristicMiner
from pymine.mining.process.network.dependency import *

class TestHeuristicMiner(unittest.TestCase):

    def __init__(self, label):
        super(TestHeuristicMiner, self).__init__(label)

    def create_log_from_test_data(self):
        the_process = Process(_id="p1")
        case1 = the_process.add_case("c1")
        case2 = the_process.add_case("c2")
        activity_a = the_process.add_activity(activity_name="A")
        activity_b = the_process.add_activity(activity_name="B")
        activity_c = the_process.add_activity(activity_name="C")
        activity_d = the_process.add_activity(activity_name="D")
        # Event 1
        activity_a_instance = case1.add_activity_instance(activity_a)
        case1.add_event(activity_a_instance, timestamp="00:00:01", resources=["R1"])
        # Event 2
        activity_b_instance = case1.add_activity_instance(activity_b)
        case1.add_event(activity_b_instance, timestamp="00:00:02", resources=["R1"])
        # Event 3
        activity_c_instance = case1.add_activity_instance(activity_c)
        case1.add_event(activity_c_instance, timestamp="00:00:03", resources=["R2"])
        # Event 4
        activity_b2_instance = case1.add_activity_instance(activity_b)
        case1.add_event(activity_b2_instance, timestamp="00:00:04", resources=["R2"])
        # Event 5
        activity_d_instance = case1.add_activity_instance(activity_d)
        case1.add_event(activity_d_instance, timestamp="00:00:05", resources=["R1"])
        # Event 6
        activity_a2_instance = case2.add_activity_instance(activity_a)
        case2.add_event(activity_a2_instance, timestamp="00:00:06", resources=["R1"])
        # Event 7
        activity_b3_instance = case2.add_activity_instance(activity_b)
        case2.add_event(activity_b3_instance, timestamp="00:00:07", resources=["R1"])
        # Event 8
        activity_d2_instance = case2.add_activity_instance(activity_d)
        case2.add_event(activity_d2_instance, timestamp="00:00:08", resources=["R1"])
        log = Log()
        log.processes.append(the_process)
        return log

    def create_dependency_graph(self):
        dgraph = DependencyGraph()
        node_a = dgraph.add_node("A")
        node_b = dgraph.add_node("B")
        node_c = dgraph.add_node("C")
        node_d = dgraph.add_node("D")
        dgraph.add_arc(node_a, node_b, "A->B", 1, 0.0)
        dgraph.add_arc(node_b, node_c, "B->C", 1, 0.0)
        dgraph.add_arc(node_c, node_b, "C->B", 1, 0.0)
        dgraph.add_arc(node_b, node_d, "B->D", 1, 0.0)
        return dgraph

    def test_mine_dependency_graph(self):
        log = self.create_log_from_test_data()
        hm = HeuristicMiner()
        dgraph = self.create_dependency_graph()
        mined_graph = hm.mine_dependency_graphs(log, 0, 0.0)[0]

        print("===================================")
        print("======= Calculated ================")
        for node in dgraph.nodes:
            print "Node: "+str(node)
        for arc in dgraph.arcs:
            print "Arc: "+str(arc)

        print("===================================")
        print("======= Mined =====================")
        for node in mined_graph.nodes:
            print "Node: "+str(node)
        for arc in mined_graph.arcs:
            print "Arc: "+str(arc)

        self.assertEqual(dgraph, mined_graph)

def suite():
    suite = unittest.TestSuite()
    suite.addTest(TestHeuristicMiner('test_mine_dependency_graph'))
    return suite

if __name__ == '__main__':
    runner = unittest.TestHeuristicMiner(verbosity=2)
    runner.run(suite())