import unittest
from pymine.mining.process.network.bpmn import BPMNDiagram, Activity, Transaction, ParallelGateway, ExclusiveGateway
from pymine.mining.process.network.bpmn.parser.xpdl_parser import XPDLParser
import os
MODULE_PATH = os.path.dirname(__file__)

class TestXPDLparser(unittest.TestCase):

    def setUp(self):
        xpdl_file = os.path.join(MODULE_PATH, 'test.xpdl')
        parser = XPDLParser(xpdl_file=xpdl_file)
        self.bpmn_network = parser.parse()

    def tearDown(self):
        pass

    def test_process_initial_activity(self):
        self.assertItemsEqual(self.bpmn_network.get_initial_nodes(), [self.bpmn_network.get_node_by_label('start_0')])

    def test_process_final_activities(self):
        self.assertItemsEqual(self.bpmn_network.get_final_nodes(),
                              [self.bpmn_network.get_node_by_label('fail_switch'),
                               self.bpmn_network.get_node_by_label('nf_switch'),
                               self.bpmn_network.get_node_by_label('fail_4'),
                               self.bpmn_network.get_node_by_label('abort_6'),
                               self.bpmn_network.get_node_by_label('end_8')
                                ])

    def test_activity_label(self):
        label = 'test_activity_3'
        self.assertEqual(self.bpmn_network.get_node_by_label(label).label, label)

    def test_incoming_transactions(self):
        act_5 = self.bpmn_network.get_node_by_label(('test_activity_5'))
        incoming = act_5.input_arcs
        self.assertItemsEqual(incoming, [self.bpmn_network.get_arc_by_label('step_3_4'), \
                                         self.bpmn_network.get_arc_by_label('test_process_tra5')])
    def test_outgoing_transactions(self):
        act_5 = self.bpmn_network.get_node_by_label(('test_activity_5'))
        outgoing = act_5.output_arcs
        self.assertItemsEqual(outgoing, [self.bpmn_network.get_arc_by_label('step_5_7'), \
                                         self.bpmn_network.get_arc_by_label('test_process_tra8')])

    def test_activity_event_attribute(self):
        act_7 = self.bpmn_network.get_node_by_label(('test_activity_7'))
        self.assertEqual(act_7.event, 'E_S0_07')

    def test_exclusive_gateway_node(self):
        gateway = self.bpmn_network.get_node_by_label('test_switch')
        self.assertIsInstance(gateway, ExclusiveGateway)



if __name__ == '__main__':
    unittest.main()
