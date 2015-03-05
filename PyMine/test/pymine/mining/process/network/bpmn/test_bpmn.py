import unittest
from pymine.mining.process.network.bpmn import BPMNDiagram, Activity, Transaction

class TestBPMNDiagram(unittest.TestCase):

    def setUp(self):
        #build a simple test diagram
        # start -> a1 -> a2 ->end
        #   |             |
        #   |_____________|

        self.bpmn_diagram = BPMNDiagram(label='test')
        start = self.bpmn_diagram.add_node('start')
        self.initial = start
        a1 = self.bpmn_diagram.add_node('a1')
        a2 = self.bpmn_diagram.add_node('a2')
        self.a2 = a2
        end = self.bpmn_diagram.add_node('end')
        self.final = end
        self.bpmn_diagram.add_arc(start, a1, 't_start_a1')
        self.bpmn_diagram.add_arc(a1, a2, 'a1_a2')
        self.bpmn_diagram.add_arc(start, a2, 'start_a2')
        self.bpmn_diagram.add_arc(a2, end, 'a2_end')

    def tearDown(self):
        pass

    def test_add_activity(self):
        test_act = self.bpmn_diagram.add_node('test')
        self.assertTrue(test_act.net, self.bpmn_diagram)

    def test_get_single_start_activity(self):
        s = self.bpmn_diagram.get_initial_nodes()
        self.assertEqual([self.initial], s)

    def test_get_single_end_activity(self):
        e = self.bpmn_diagram.get_final_nodes()
        self.assertEqual([self.final], e)

    def test_get_multiple_end_activity(self):
        end_2 = self.bpmn_diagram.add_node('multiple_end')
        self.bpmn_diagram.add_arc(self.a2, end_2, 'end_arc_2')
        self.assertEqual([self.final, end_2], self.bpmn_diagram.get_final_nodes())

    def test_get_activity_by_id(self):
         get_a2 = self.bpmn_diagram.get_node_by_label('a2')
         self.assertEqual(get_a2, self.a2)

    # def test_add_activity_same_id(self):
    #     self.assertRaises(Exception, self.bpmn_diagram.add_activity, Activity(id='a2'))
    #
    # def test_add_transaction_same_id(self):
    #     t = Transaction(id='t_start_a1')
    #     self.assertRaises(Exception, self.bpmn_diagram.add_transaction, t)
    #
    # def test_add_yet_present_transaction(self):
    #     self.assertRaises(Exception, self.bpmn_diagram.add_transaction, Transaction(self.start, self.a1))


# # def suite():
# #     suite = unittest.TestSuite()
# #     #suite.addTest(TestBPMNDiagram('test_add_activity'))
# #     #suite.addTest(TestBPMNDiagram('test_get_start_activity'))
# #     #suite.addTest(TestBPMNDiagram('test_get_end_activity'))
# #     #suite.addTest(TestBPMNDiagram('test_get_activity_by_id'))
# #     suite.addTest(TestBPMNDiagram('test_add_activity_same_id'))
#
#     return suite

if __name__ == '__main__':
    unittest.main()






