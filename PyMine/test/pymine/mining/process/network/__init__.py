import unittest
from pymine.mining.process.network import Network, Node, Arc


class NetworkTestCase(unittest.TestCase):

    def test_add_node(self):
        net = Network()
        a = net.add_nodes('a')
        print a
        self.assertTrue(isinstance(a, Node))
        self.assertTrue(a.label == 'a')

    def test_add_nodes(self):
        net = Network()
        a, b = net.add_nodes('a', 'b')
        self.assertTrue(a.label == 'a')
        self.assertTrue(b.label == 'b')
