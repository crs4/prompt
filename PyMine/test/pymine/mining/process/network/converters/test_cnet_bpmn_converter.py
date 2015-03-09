import unittest
from pymine.mining.process.network import Arc
from pymine.mining.process.network.bpmn import BPMNDiagram, Activity, Transaction, ParallelGateway
from pymine.mining.process.network.cnet import CNet
from pymine.mining.process.network.converters.cnet_bpmn_converter import CNetBPMNConverter

class TestCNETBPMNConverter(unittest.TestCase):
    def setUp(self):
        #define testing CNet
        net = CNet()
        a0,b1,c2,d3 = net.add_nodes('0', '1', '2','3')
        bind_0_12 = net.add_output_binding(a0, {b1,c2})
        bind_1_3 = net.add_output_binding(b1, {d3})
        bind_1_0 = net.add_input_binding(b1, {a0})
        bind_2_3 = net.add_output_binding(c2, {d3})
        bind_2_0 = net.add_input_binding(c2, {a0})
        bind_3_12 = net.add_input_binding(d3, {b1,c2})
        converter = CNetBPMNConverter(net)
        self.bpmn = converter.convert_to_BPMN()


    def tearDown(self):
        pass

    def test_network_compliance(self):
        #The set-up C-net should be converted to a BPMN diagram, composed by the following activities an gateways:
        #       -   activities 0,1,2,3
        #       -   gateways: 2 parallel gateways, called for convenience gw_0 and gw_3
        #The following connections must be present:
        #       - 0 -> gw_0
        #       - gw_0 -> 1
        #       - gw_0 -> 2
        #       - 1 -> gw_3
        #       - 2 -> gw_3
        #       - gw_3 ->3

        #get gateway nodes label (they have an automatic label which starts with the node collected to the gateway at input or output)
        gw_0_label = ''
        gw_3_label = ''
        for n in self.bpmn.nodes:
            if n.label[0] == '0' and len (n.label) > 1:
                gw_0_label = n.label
            elif n.label[0] == '3' and len (n.label) > 1:
                gw_3_label = n.label
        self.assertEqual(len(self.bpmn.nodes), 6)
        self.assertEqual(len(self.bpmn.arcs), 6)
        gw_0 = self.bpmn.get_node_by_label(gw_0_label)
        self.assertIsInstance(gw_0, ParallelGateway)
        self.assertIsInstance(self.bpmn.get_arc_by_nodes(self.bpmn.get_node_by_label('0'), self.bpmn.get_node_by_label(gw_0_label)), Arc)
        self.assertIsInstance(self.bpmn.get_arc_by_nodes(self.bpmn.get_node_by_label(gw_0_label), self.bpmn.get_node_by_label('1')), Arc)
        self.assertIsInstance(self.bpmn.get_arc_by_nodes(self.bpmn.get_node_by_label(gw_0_label), self.bpmn.get_node_by_label('2')), Arc)
        gw_3 = self.bpmn.get_node_by_label(gw_3_label)
        self.assertIsInstance(gw_3, ParallelGateway)
        self.assertIsInstance(self.bpmn.get_arc_by_nodes(self.bpmn.get_node_by_label('1'), self.bpmn.get_node_by_label(gw_3_label)), Arc)
        self.assertIsInstance(self.bpmn.get_arc_by_nodes(self.bpmn.get_node_by_label('2'), self.bpmn.get_node_by_label(gw_3_label)), Arc)
        self.assertIsInstance(self.bpmn.get_arc_by_nodes(self.bpmn.get_node_by_label(gw_3_label), self.bpmn.get_node_by_label('3')), Arc)


if __name__ == '__main__':
    unittest.main()