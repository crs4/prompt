import unittest
from pymine.mining.process.network.cnet import CNet
from pymine.mining.process.eventlog.factory import DictLogFactory
import pymine.conformance.alignment as aln


class AlignmentTestCase(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(AlignmentTestCase, self).__init__(*args, **kwargs)
        self.net = CNet()
        a, b, c = self.net.add_nodes('a', 'b', 'c')
        self.net.add_output_binding(a, {b})
        self.net.add_input_binding(b, {a})
        self.net.add_output_binding(b, {c})
        self.net.add_input_binding(c, {b})

    def test_optimal_alignment(self):

        case = ['a', 'b', 'c']
        log = DictLogFactory({'test': [case]})
        alignment = aln.compute_optimal_alignment(log.cases[0], self.net)
        self.assertEqual(alignment.cost, 0)
        self.assertEqual(alignment.get_flat_log_moves(), case)
        self.assertEqual(alignment.get_flat_net_moves(), case)
        

if __name__ == '__main__':
    unittest.main()

