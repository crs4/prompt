import unittest
from pymine.mining.process.network.bpmn.bpmn import BPMNDiagram, Activity, Transaction

class TestBPMNDiagram(unittest.TestCase):

    def setUp(self):
        #build a simple test diagram
        # start -> a1 -> a2 ->end
        #   |             |
        #   |_____________|
        start = Activity(id='start')
        start.is_start = True
        self.start = start
        a1 = Activity(id='a1')
        self.a1 = a1
        a2 = Activity(id='a2')
        self.a2 = a2
        end = Activity(id='end')
        end.is_end = True
        self.end = end
        t_start_a1 = Transaction(id = 't_start_a1', from_act=start, to_act=a2)
        t_a1_a2 = Transaction(id = 'a1_a2', from_act=a1, to_act=a2)
        t_start_a2 = Transaction(id = 'start_a2', from_act = start, to_act = a2)
        t_a2_end = Transaction(id = 'a2_end', from_act = a2, to_act = end)
        self.bpmn_diagram = BPMNDiagram(process_id = 'test', process_activities=[start, a1, a2, end], process_transactions=[ \
                                   t_start_a1, t_a1_a2, t_start_a2, t_a2_end])
        self.end = end

    def tearDown(self):
        pass

    def test_add_activity(self):
        test_diagram = self.bpmn_diagram
        test_act = Activity(id='test')
        test_diagram.add_activity(test_act)
        self.assertIn(test_act, test_diagram.process_activities)

    def test_get_start_activity(self):
        s = self.bpmn_diagram.get_start_activity()
        self.assertEqual(s.id, 'AC_start')

    def test_get_end_activity(self):
        e = self.bpmn_diagram.get_end_activities()
        self.assertEqual(e, [self.end])

    def test_add_transaction(self):
        test_diagram = self.bpmn_diagram
        new_tr = Transaction(id='a1_end',from_act=self.a1, to_act = self.end)
        test_diagram.add_transaction(new_tr)
        self.assertIn(new_tr, test_diagram.process_transactions)

    def test_get_activity_by_id(self):
        get_a2 = self.bpmn_diagram.get_activity_by_id('a2')
        self.assertEqual(get_a2, self.a2)

    def test_add_activity_same_id(self):
        self.assertRaises(Exception, self.bpmn_diagram.add_activity, Activity(id='a2'))

    def test_add_transaction_same_id(self):
        t = Transaction(id='t_start_a1')
        self.assertRaises(Exception, self.bpmn_diagram.add_transaction, t)

    def test_add_yet_present_transaction(self):
        self.assertRaises(Exception, self.bpmn_diagram.add_transaction, Transaction(self.start, self.a1))


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






