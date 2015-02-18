import unittest
from pymine.mining.process.network import Network, Node, Arc


class NetworkTestCase(unittest.TestCase):

    def test_add_node(self):
        net = Network()
        a = net.add_nodes('a')

        self.assertTrue(isinstance(a, Node))
        self.assertTrue(a.label == 'a')
        self.assertTrue(net.nodes == [a])
        self.assertTrue(net.get_node_by_label('a') == a)

    def test_add_nodes(self):
        net = Network()
        a, b = net.add_nodes('a', 'b')
        self.assertTrue(a.label == 'a')
        self.assertTrue(b.label == 'b')
        self.assertTrue(set(net.nodes) == {a, b})

    def test_add_arc(self):
        net = Network()
        a, b = net.add_nodes('a', 'b')
        arc = net.add_arc(a, b, 'arc')

        self.assertTrue(net.arcs == [arc])
        self.assertTrue(arc.label == 'arc')
        self.assertTrue(len(a.input_arcs) == 0)
        self.assertTrue(set(a.output_arcs) == {arc})
        self.assertTrue(arc.input_node == b)
        self.assertTrue(arc.output_node == a)
