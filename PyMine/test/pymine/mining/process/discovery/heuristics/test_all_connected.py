import unittest
from test.pymine.mining.process.discovery.heuristics import BackendTests
from pymine.mining.process.discovery.heuristics.all_connected import HeuristicMiner
import logging
logging.basicConfig(format="%(filename)s %(lineno)s %(levelname)s: %(message)s")
logger = logging.getLogger('all_connected')
# logger.setLevel(logging.DEBUG)


from pymine.mining.process.eventlog.factory import create_process_log_from_list, create_log_from_file


def get_binding_set(binding):
    return {frozenset(bi.node_set) for bi in binding}


class TestAllConnected(BackendTests, unittest.TestCase):
    def create_miner(self, log):
        return HeuristicMiner(log)

    def test_2_step_loop(self):
        log = create_process_log_from_list([
            ['a', 'b', 'c', 'd'],
            ['a', 'b', 'c', 'b', 'c', 'd'],
            ['a', 'b', 'c', 'b', 'c', 'b', 'c', 'd'],

        ])
        miner = self.create_miner(log)
        cnet = miner.mine(bindings_thr=0.2)
        a, b, c, d = [cnet.get_node_by_label(n) for n in ['a', 'b', 'c', 'd']]
        self.assertEqual(cnet.get_initial_nodes(), [a])
        self.assertEqual(cnet.get_final_nodes(), [d])

        self.assertEqual(get_binding_set(a.output_bindings), {frozenset({b})})

        self.assertEqual(get_binding_set(b.input_bindings), {frozenset({a}), frozenset({c})})
        self.assertEqual(get_binding_set(b.output_bindings), {frozenset({c})})

        self.assertEqual(get_binding_set(c.input_bindings), {frozenset({b})})
        self.assertEqual(get_binding_set(c.output_bindings), {frozenset({b}), frozenset({d})})

        self.assertEqual(get_binding_set(d.input_bindings), {frozenset({c})})

        b_c_bin = b.get_output_bindings_with({c}, True)
        self.assertEqual(b_c_bin.frequency, 6)

        b_c_bin = b.get_input_bindings_with({c}, True)
        self.assertEqual(b_c_bin.frequency, 3)

        c_b_bin = c.get_output_bindings_with({b}, True)
        self.assertEqual(c_b_bin.frequency, 3)

        c_b_bin = c.get_input_bindings_with({b}, True)
        self.assertEqual(c_b_bin.frequency, 6)