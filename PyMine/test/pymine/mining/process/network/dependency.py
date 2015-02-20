import unittest
from pymine.mining.process.network.dependency import DependencyGraph, DArc


class DependencyTestCase(unittest.TestCase):

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

        self.assertTrue(arc.input_node == b)
        self.assertTrue(arc.output_node == a)

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
