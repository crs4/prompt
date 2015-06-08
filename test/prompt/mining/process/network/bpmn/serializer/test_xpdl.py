import unittest
from prompt.mining.process.network.bpmn import BPMNDiagram, ParallelGateway, ExclusiveGateway
from prompt.mining.process.network.bpmn.serializer.xpdl import XPDLSerializer
from xml.etree import ElementTree

class TestXPDLSerializer(unittest.TestCase):

    def setUp(self):
        #define test BPMN: it is the BPMN book example
        test_bpmn = BPMNDiagram('test')
        #------------------Define nodes(Activities)------------------------------
        start, register_request, examine_throughly, examine_casually = \
            test_bpmn.add_nodes('start', 'register_request', 'examine_throughly', 'examine_casually')
        check_ticket, reinitiate_request, decide = test_bpmn.add_nodes('check_ticket', 'reinitiate_request', 'decide')
        pay_compensation, reject_request, end, end_bpmn = test_bpmn.add_nodes('pay_compensation', 'reject_request', 'end', 'end_bpmn' )
        #-------------------Define nodes (Gateway)-------------------------------
        register_request_O = test_bpmn.add_node(label='register_request_O', node_type='exclusive_gateway')
        register_request_O0 = test_bpmn.add_node(label='register_request_O0', node_type='parallel_gateway')
        register_request_O1 = test_bpmn.add_node(label='register_request_O1', node_type='parallel_gateway')
        decide_I0 = test_bpmn.add_node(label='decide_I0', node_type='parallel_gateway')
        decide_O = test_bpmn.add_node(label='decide_O', node_type='exclusive_gateway')
        reinitiate_request_O = test_bpmn.add_node(label='reinitiate_request_O', node_type='exclusive_gateway')
        reinitiate_request_O0 = test_bpmn.add_node(label='reinitiate_request_O0', node_type='parallel_gateway')
        reinitiate_request_O1 = test_bpmn.add_node(label='reinitiate_request_O1', node_type='parallel_gateway')
        check_ticket_decide = test_bpmn.add_node(label='check_ticket_decide', node_type='exclusive_gateway')
        decide_I1 = test_bpmn.add_node(label='decide_I1', node_type = 'parallel_gateway')
        #-----------------------------------------------------------------------------
        #--------------------Define arcs (Transactions)-------------------------------
        test_bpmn.add_arc(start, register_request)
        test_bpmn.add_arc(register_request, register_request_O)
        test_bpmn.add_arc(register_request_O, register_request_O0)
        test_bpmn.add_arc(register_request_O, register_request_O1)
        test_bpmn.add_arc(register_request_O0, check_ticket)
        test_bpmn.add_arc(register_request_O0, examine_throughly)
        test_bpmn.add_arc(register_request_O1, examine_casually)
        test_bpmn.add_arc(register_request_O1, check_ticket)
        test_bpmn.add_arc(examine_throughly, decide_I0)
        test_bpmn.add_arc(decide_I0, decide)
        test_bpmn.add_arc(decide, decide_O)
        test_bpmn.add_arc(decide_O, pay_compensation)
        test_bpmn.add_arc(decide_O, reject_request)
        test_bpmn.add_arc(decide_O, reinitiate_request)
        test_bpmn.add_arc(pay_compensation, end)
        test_bpmn.add_arc(reject_request, end)
        test_bpmn.add_arc(end, end_bpmn)
        test_bpmn.add_arc(reinitiate_request,reinitiate_request_O)
        test_bpmn.add_arc(reinitiate_request_O, reinitiate_request_O0)
        test_bpmn.add_arc(reinitiate_request_O, reinitiate_request_O1)
        test_bpmn.add_arc(reinitiate_request_O0, check_ticket)
        test_bpmn.add_arc(reinitiate_request_O0, examine_throughly)
        test_bpmn.add_arc(reinitiate_request_O1, check_ticket)
        test_bpmn.add_arc(reinitiate_request_O1, examine_casually)
        test_bpmn.add_arc(check_ticket, check_ticket_decide)
        test_bpmn.add_arc(check_ticket_decide, decide_I1)
        test_bpmn.add_arc(decide_I1, decide)
        test_bpmn.add_arc(examine_casually, decide_I1)
        self.bpmn = test_bpmn
        s = XPDLSerializer(test_bpmn)
        s.serialize()
        xpdl = s.root
        self.xpdl = xpdl

    def tearDown(self):
        pass

    def test_xpdl_structure(self):
        #check root node presence
        root = self.xpdl.iter('Package')
        self.assertEqual(len(list(root)), 1)
        #check sub-elements presence
        package_header = self.xpdl.iter('PackageHeader').next()
        self.assertEqual(len(list(package_header)), 2)
        for ch in package_header.getchildren():
            self.assertIn(ch.tag, ['XPDLVersion', 'Created'])
        #check for Workflow processes, at the moment only 1 WF process
        wf_processes = self.xpdl.iter('WorkflowProcesses').next()
        for ch in wf_processes.getchildren():
            self.assertEqual([ch.tag], ['WorkflowProcess'])
        #Activities
        activities = self.xpdl.iter('Activities').next()
        for ch in activities.getchildren():
            self.assertEqual([ch.tag], ['Activity'])
        #Transitions
        transitions = self.xpdl.iter('Transitions').next()
        for ch in transitions.getchildren():
            self.assertEqual([ch.tag], ['Transition'])

    def test_check_activities(self):
        bpmn_act = self.bpmn.get_activities()
        act_labels = [n.label for n in bpmn_act]
        xpdl_activities = self.xpdl.iter('Activities').next()
        counter_acts = 0
        for a in xpdl_activities.getchildren():
            if not a.getchildren() or not (a.getchildren()[0].tag == 'Route'):
                self.assertIn(a.get('Id'), act_labels)
                counter_acts += 1
        self.assertEqual(counter_acts, len(bpmn_act))

    def test_check_gateways(self):
        bpmn_gw = self.bpmn.get_gateways()
        gw_labels = [gw.label for gw in bpmn_gw]
        xpdl_gws = self.xpdl.iter('Activities').next()
        counter_gws = 0
        for gw in xpdl_gws.getchildren():
            if gw.getchildren() and (gw.getchildren()[0].tag == 'Route'):
                self.assertIn(gw.get('Id'), gw_labels)
                if gw.getchildren()[0].get('GatewayType') == 'Exclusive':
                    self.assertIsInstance(self.bpmn.get_node_by_label(gw.get('Id')), ExclusiveGateway)
                elif gw.getchildren()[0].get('GatewayType') == 'Parallel':
                    self.assertIsInstance(self.bpmn.get_node_by_label(gw.get('Id')), ParallelGateway)
                counter_gws +=1
        self.assertEqual(counter_gws, len(bpmn_gw))

    def test_check_transitions(self):
        bpmn_transitions = list()
        for a in self.bpmn.arcs:
            bpmn_transitions.append(a.start_node.label+'-->'+a.end_node.label)
        xpdl_transitions = self.xpdl.iter('Transitions').next()
        counter_tr = 0
        for transition in xpdl_transitions.getchildren():
            t = transition.get('From')+'-->'+transition.get('To')
            self.assertIn(t, bpmn_transitions)
            counter_tr+=1
        self.assertEqual(counter_tr, len(self.bpmn.arcs))


if __name__ == '__main__':
    unittest.main()