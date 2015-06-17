import unittest
from prompt.mining.process.network import Node, Arc
from prompt.mining.process.network.bpmn import BPMNDiagram, Activity, Transaction, ParallelGateway, ExclusiveGateway
from prompt.mining.process.network.cnet import CNet
from prompt.mining.process.network.converters.cnet_bpmn_converter import CNetBPMNConverter


class TestCNETBPMNConverter(unittest.TestCase):
    def setUp(self):
        #define first testing CNet
        m_net = CNet()
        a, b, c, d, e, f = m_net.add_nodes('a', 'b', 'c','d', 'e', 'f')
        m_net.add_output_binding(a, {b,c,d})
        m_net.add_output_binding(a, {b,c,e})
        m_net.add_input_binding(b, {a})
        m_net.add_input_binding(c, {a})
        m_net.add_input_binding(d, {a})
        m_net.add_input_binding(e, {a})
        m_net.add_output_binding(c, {f})
        m_net.add_output_binding(b, {f})
        m_net.add_output_binding(d, {f})
        m_net.add_output_binding(e, {f})
        m_net.add_input_binding(f, {b,c,e})
        m_net.add_input_binding(f, {b,c,d})
        converter = CNetBPMNConverter(m_net)
        self.bpmn_first = converter.convert_to_BPMN()

        #define second test cnet
        net_b = CNet()
        a, b, c, d, e, f, g, h, z = net_b.add_nodes('a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'z')
        net_b.add_output_binding(a, {b,d})
        net_b.add_output_binding(a, {c,d})
        net_b.add_output_binding(b, {e})
        net_b.add_input_binding(b, {a})
        net_b.add_input_binding(b, {f})
        net_b.add_output_binding(c, {e})
        net_b.add_input_binding(c, {a})
        net_b.add_input_binding(c, {f})
        net_b.add_output_binding(d, {e})
        net_b.add_input_binding(d, {a})
        net_b.add_input_binding(d, {f})
        net_b.add_output_binding(e, {g})
        net_b.add_output_binding(e, {h})
        net_b.add_output_binding(e, {f})
        net_b.add_input_binding(e, {b,d})
        net_b.add_input_binding(e, {c,d})
        net_b.add_output_binding(f, {b,d})
        net_b.add_output_binding(f, {c,d})
        net_b.add_input_binding(f, {e})
        net_b.add_output_binding(g, {z})
        net_b.add_input_binding(g, {e})
        net_b.add_output_binding(h, {z})
        net_b.add_input_binding(h, {e})
        net_b.add_input_binding(z, {g})
        net_b.add_input_binding(z, {h})
        c_book = CNetBPMNConverter(net_b)
        self.bpmn_second = c_book.convert_to_BPMN()


    def test_assert_first_network_compliance(self):
        """
        According to Prom algorithm, the network must have the following structure: for each row is indicated:
        current node, class type, list of previous nodes, list of following nodes
        ('START', Node, [], [a])
        ('a', Node, ['START'], ['a_O'])
        ('a_O', ExclusiveGateway, ['a'], ['a_O0',a_01])
        ('a_O0', ParallelGateway, ['a_O'], ['b','c','e'])
        ('a_O1', ParallelGateway, ['a_O'], ['d', 'b', 'c'])
        ('b', Node, ['a_O0','a_O1'], ['b_f'] )
        ('c', Node, ['a_O0','a_O1'], ['c_f'] )
        ('d', Node, ['a_O0','a_O1'], ['f_I1'] )
        ('e', Node, ['a_O0','a_O1'], ['f_I0'] )
        ('b_f', ExclusiveGateway, ['b'], ['f_I0',f_I1])
        ('c_f', ExclusiveGateway, ['c'], ['f_I0',f_I1])
        ('f_I0', ParallelGateway, ['b_f','c_f','e'], ['f'])
        ('f_I1', ParallelGateway, ['b_f','c_f','d'], ['f'])
        ('f', Node,  ['f_I0', 'f_I1'], ['END'])
        ('END', Node, ['f'], [])
        :return:
        """
        structure = {}
        structure['START'] = (Node, [], ['a'])
        structure['a'] = (Node, ['START'], ['a_O'])
        structure['a_O']  = (ExclusiveGateway, ['a'], ['a_O0','a_O1'])
        structure['a_O0'] = (ParallelGateway, ['a_O'], ['b','c','d'])
        structure['a_O1'] = (ParallelGateway, ['a_O'], ['e', 'b', 'c'])
        structure['b'] = (Node, ['a_O0','a_O1'], ['b_f'] )
        structure['c'] = (Node, ['a_O0','a_O1'], ['c_f'] )
        structure['d'] = (Node, ['a_O0'], ['f_I1'] )
        structure['e'] = (Node, ['a_O1'], ['f_I0'] )
        structure['b_f'] = (ExclusiveGateway, ['b'], ['f_I0','f_I1'])
        structure['c_f'] = (ExclusiveGateway, ['c'], ['f_I0','f_I1'])
        structure['f_I0'] = (ParallelGateway, ['b_f','c_f','e'], ['f'])
        structure['f_I1'] = (ParallelGateway, ['b_f','c_f','d'], ['f'])
        structure['f'] = (Node,  ['f_I0', 'f_I1'], ['END'])
        structure['END'] = (Node, ['f'], [])
        self.assertEqual(len(self.bpmn_first.nodes), 15)
        self.assertEqual(len(self.bpmn_first.arcs), 21)
        for n in self.bpmn_first.nodes:
            node_info = structure[n.label]
            self.assertTrue(structure.has_key(n.label))
            self.assertIsInstance(n, node_info[0])
            self.assertItemsEqual([input.label for input in n.input_nodes], node_info[1])
            self.assertItemsEqual([output.label for output in n.output_nodes], node_info[2])

    def test_assert_second_network_compliance(self):
        """
        This one is a quite complex C-Net with more nodes, acts and bindings than the previous one.
        :return:
        """
        structure = {}
        structure['START'] = (Node, [], ['a'])
        structure['a'] = (Node, ['START'], ['a_O'])
        structure['a_O'] = (ExclusiveGateway, ['a'], ['a_O0', 'a_O1'])
        structure['a_O0'] = (ParallelGateway, ['a_O'], ['d','b'])
        structure['a_O1'] = (ParallelGateway, ['a_O'], ['c','d'])
        structure['b'] = (Node, ['a_O0', 'f_O0'], ['e_I0'])
        structure['c'] = (Node, ['a_O1', 'f_O1'], ['e_I1'])
        structure['d'] = (Node, ['a_O0', 'a_O1', 'f_O0', 'f_O1'], ['d_e'])
        structure['d_e'] = (ExclusiveGateway, ['d'], ['e_I1','e_I0'])
        structure['e_I0'] = (ParallelGateway, ['d_e', 'b'], ['e'])
        structure['e_I1'] = (ParallelGateway, ['d_e', 'c'], ['e'])
        structure['e'] = (Node, ['e_I0', 'e_I1'], ['e_O'])
        structure['e_O'] = (ExclusiveGateway, ['e'], ['f', 'g', 'h'])
        structure['f'] = (Node, ['e_O'], ['f_O'])
        structure['g'] = (Node, ['e_O'], ['z'])
        structure['h'] = (Node, ['e_O'], ['z'])
        structure['f_O'] = (ExclusiveGateway, ['f'], ['f_O0', 'f_O1'])
        structure['f_O0'] = (ParallelGateway, ['f_O'], ['d','b'])
        structure['f_O1'] = (ParallelGateway, ['f_O'], ['c', 'd'])
        structure['z'] = (Node, ['g', 'h'], ['END'])
        structure['END'] = (Node, ['z'], [])
        self.assertEqual(len(self.bpmn_second.nodes), 21)
        self.assertEqual(len(self.bpmn_second.arcs), 29)
        for n in self.bpmn_second.nodes:
            node_info = structure[n.label]
            self.assertTrue(structure.has_key(n.label))
            self.assertIsInstance(n, node_info[0])
            self.assertItemsEqual([input.label for input in n.input_nodes], node_info[1])
            self.assertItemsEqual([output.label for output in n.output_nodes], node_info[2])

    def tearDown(self):
        pass




if __name__ == '__main__':
    unittest.main()