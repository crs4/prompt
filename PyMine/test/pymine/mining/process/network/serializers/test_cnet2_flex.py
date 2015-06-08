import unittest, os
from pymine.mining.process.network.cnet import CNet
from pymine.mining.process.network.serializers.cnet2flex import CNet2FlexSerializer, FlexCNetLoader

class TestCnet2Flex(unittest.TestCase):
    #obtain the file from the cnet, then load it in order to obtain back the starting Cnet

    def setUp(self):
        self.test_dir = './testfiles/'
        if not os.path.isdir(self.test_dir) :
            os.mkdir(self.test_dir)

        m_net = CNet('test_cnet')
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
        self.cnet = m_net


    def test_cnet_to_flex(self):
        cnet_2_flex = CNet2FlexSerializer(self.cnet)
        cnet_2_flex.serialize()
        cnet_2_flex.create_flex_file(self.test_dir+'testfile.flex', is_pretty=True)

    def test_flex_to_cnet(self):
        flex_to_cnet = FlexCNetLoader(self.test_dir+'testfile.flex')
        cnet_l = flex_to_cnet.load()
        labels = [n.label for n in self.cnet.nodes]
        #check number of nodes
        self.assertEqual(len(cnet_l.nodes), len(self.cnet.nodes))
        #check nodes
        for n in cnet_l.nodes:
            self.assertIn(n.label, labels)
        #check number of arcs
        self .assertEqual(len(cnet_l.arcs), len(self.cnet.arcs))
        #check output and input bindings for each node
        for l in labels:
            node_cnet = self.cnet.get_node_by_label(l)
            node_flex = cnet_l.get_node_by_label(l)
            input_bindings_cnet = node_cnet.input_bindings
            output_bindings_cnet = node_cnet.output_bindings
            input_bindings_flex = node_flex.input_bindings
            output_bindings_flex = node_flex.output_bindings
            input_cnet = []
            input_flex = []
            output_cnet = []
            output_flex = []
            bind = set()
            for b in input_bindings_cnet:
                bind = set()
                for s in b.node_set:
                    bind.add(s.label)
                input_cnet.append(bind)
            for b in input_bindings_flex:
                bind = set()
                for s in b.node_set:
                    bind.add(s.label)
                input_flex.append(bind)
            self.assertEqual(input_cnet, input_flex)
            for b in output_bindings_cnet:
                bind = set()
                for s in b.node_set:
                    bind.add(s.label)
                output_cnet.append(bind)
            for b in output_bindings_flex:
                bind = set()
                for s in b.node_set:
                    bind.add(s.label)
                output_flex.append(bind)
            self.assertEqual(output_cnet, output_flex)

if __name__ == '__main__':
    unittest.main()
