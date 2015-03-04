import unittest
from pymine.mining.process.network.cnet import CNet
from pymine.mining.process.eventlog.factory import DictLogFactory
import pymine.conformance.alignment as aln
from pymine.conformance.alignment import Alignment
import logging
logging.basicConfig(level=logging.DEBUG, format="%(filename)s %(lineno)s %(levelname)s: %(message)s")


class AlignmentTestCase(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(AlignmentTestCase, self).__init__(*args, **kwargs)
        self.net = CNet()
        a, b, c, d = self.net.add_nodes('a', 'b', 'c', 'd')
        self.net.add_output_binding(a, {b, c})
        self.net.add_input_binding(b, {a})
        self.net.add_input_binding(c, {a})

        self.net.add_output_binding(b, {d})
        self.net.add_output_binding(c, {d})
        self.net.add_input_binding(d, {b, c})

        self.double_cost_function = lambda lm, nm: 0 if lm == nm else 2

    def test_optimal_alignment(self):

        case = ['a', 'b', 'c', 'd']
        log = DictLogFactory({'test': [case]})
        alignment = aln.compute_optimal_alignment(log.cases[0], self.net)
        logging.debug('alignment.cost %s', alignment.cost)
        logging.debug('alignment.get_flat_log_moves() %s', alignment.get_flat_log_moves())
        logging.debug('alignment.get_flat_net_moves() %s', alignment.get_flat_net_moves())

        self.assertEqual(alignment.cost, 0)
        self.assertEqual(alignment.get_flat_log_moves(), case)
        self.assertEqual(alignment.get_flat_net_moves(), case)

    def test_optimal_alignment_with_cost_function(self):

        case = ['a', 'b', 'd']
        log = DictLogFactory({'test': [case]})
        self.double_cost_function = lambda lm, nm: 0 if lm == nm else 2
        alignment = aln.compute_optimal_alignment(log.cases[0], self.net, self.double_cost_function)
        logging.debug('alignment.cost %s', alignment.cost)
        logging.debug('alignment.get_flat_log_moves() %s', alignment.get_flat_log_moves())
        logging.debug('alignment.get_flat_net_moves() %s', alignment.get_flat_net_moves())

        self.assertEqual(alignment.cost, 2)
        self.assertEqual(alignment.get_flat_log_moves(), ['a', 'b', Alignment.null_move, 'd'])
        self.assertEqual(alignment.get_flat_net_moves(), ['a', 'b', 'c', 'd'])

    def test_optimal_alignment_worst_scenario(self):

        case = ['w', 'y', 'z']
        log = DictLogFactory({'test': [case]})
        alignment = aln.compute_optimal_alignment(log.cases[0], self.net)
        logging.debug('alignment.cost %s', alignment.cost)
        logging.debug('alignment.get_flat_log_moves() %s', alignment.get_flat_log_moves())
        logging.debug('alignment.get_flat_net_moves() %s', alignment.get_flat_net_moves())
        #
        self.assertEqual(alignment.cost, len(case) + len(self.net.nodes))
        self.assertEqual(alignment.get_flat_log_moves(), case + [Alignment.null_move]*len(self.net.nodes))

        cost, shortest_path = self.net.shortest_path()
        self.assertEqual(alignment.get_flat_net_moves(), [Alignment.null_move]*len(case) +
                         [n.label for n in shortest_path])

    def test_alignment_complete(self):
        case = ['a', 'b', 'c', 'd']
        log = DictLogFactory({'test': [case]})
        alignment = aln.case_alignment(log.cases[0], self.net)
        self.assertEqual(alignment, 1)

    def test_alignment_2(self):
        case = ['a', 'b', 'd']
        log = DictLogFactory({'test': [case]})
        alignment = aln.case_alignment(log.cases[0], self.net)
        self.assertEqual(alignment, 1 - 1.0/(4 + 3))

    def test_alignment__with_cost(self):
        case = ['a', 'b', 'd']
        log = DictLogFactory({'test': [case]})
        alignment = aln.case_alignment(log.cases[0], self.net, self.double_cost_function)
        self.assertEqual(alignment, 1 - 1.0/(4 + 3))



if __name__ == '__main__':
    unittest.main()

