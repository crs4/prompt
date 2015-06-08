import unittest
import prompt.mining.process.network.dependency
from prompt.mining.process.network.dependency import DependencyGraph, DArc


class DependencyTestCase(unittest.TestCase):

    def create_dependency_graph(self):
        graph = DependencyGraph()
        node_a = graph.add_node("A")
        node_b = graph.add_node("B")
        node_c = graph.add_node("C")
        node_d = graph.add_node("D")
        graph.add_arc(node_a, node_b, "A->B", 2)
        graph.add_arc(node_b, node_c, "B->C", 1)
        graph.add_arc(node_c, node_b, "C->B", 1)
        graph.add_arc(node_b, node_d, "B->D", 2)
        return graph

    def test_add_arc(self):
        net = DependencyGraph()
        a, b = net.add_nodes('a', 'b')
        arc = net.add_arc(a, b, 'arc')

        self.assertTrue(net.arcs == [arc])
        self.assertTrue(arc.label == 'arc')
        self.assertTrue(isinstance(arc, DArc))

        self.assertTrue(len(a.input_arcs) == 0)
        self.assertTrue(set(a.output_arcs) == {arc})

        self.assertTrue(len(b.output_arcs) == 0)
        self.assertTrue(set(b.input_arcs) == {arc})

        self.assertTrue(arc.start_node == a)
        self.assertTrue(arc.end_node == b)

        self.assertTrue(net.get_initial_nodes() == [a])
        self.assertTrue(net.get_final_nodes() == [b])

    def test_add_arc_full_args(self):
        net = DependencyGraph()
        a, b = net.add_nodes('a', 'b')
        freq = 1
        dep = 1.0
        attrs = {'test': True}
        arc = net.add_arc(a, b, 'arc', freq, dep, attrs)

        self.assertTrue(arc.frequency == freq)
        self.assertTrue(arc.dependency == dep)
        self.assertTrue(arc.attrs == attrs)

    def test_get_dependency_graph_from_json(self):
        orig_graph = self.create_dependency_graph()
        json = orig_graph.get_json()
        the_graph = prompt.mining.process.network.dependency.get_dependency_graph_from_json(json)
        self.assertTrue(json == the_graph.get_json())