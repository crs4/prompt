import unittest
from test.pymine.mining.process.discovery.heuristics import BackendTests
from pymine.mining.process.discovery.heuristics.all_connected import HeuristicMiner
import logging
import os
from pymine.mining.process.eventlog.factory import create_log_from_file
logging.basicConfig(format="%(filename)s %(lineno)s %(levelname)s: %(message)s")
logger = logging.getLogger('all_connected')


def get_binding_set(binding):
    return {frozenset(bi.node_set) for bi in binding}


class TestAllConnected(BackendTests, unittest.TestCase):
    def create_miner(self, log):
        return HeuristicMiner(log)


    def test_pg_4_dataset(self):
        dataset_path = os.path.join(os.path.dirname(__file__), '../../../../../../dataset/pg_4_label_final_node.csv')
        log = create_log_from_file(dataset_path, False, False, False)
        miner = self.create_miner(log)
        cnet = miner.mine(long_distance_thr=1)
        a, b, c, d, e, f, g, h, z = [cnet.get_node_by_label(n) for n in ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'z']]
        self.assertEqual(cnet.get_initial_nodes(), [a])
        self.assertEqual(cnet.get_final_nodes(), [z])

        self.assertEqual(get_binding_set(a.output_bindings), {frozenset({b, d}), frozenset({c, d})})

        self.assertEqual(get_binding_set(b.input_bindings), {frozenset({a}), frozenset({f})})
        self.assertEqual(get_binding_set(b.output_bindings), {frozenset({e})})

        self.assertEqual(get_binding_set(c.input_bindings), {frozenset({a}), frozenset({f})})
        self.assertEqual(get_binding_set(c.output_bindings), {frozenset({e})})

        self.assertEqual(get_binding_set(d.input_bindings), {frozenset({a}), frozenset({f})})
        self.assertEqual(get_binding_set(d.output_bindings), {frozenset({e})})

        self.assertEqual(get_binding_set(e.input_bindings), {frozenset({b, d}), frozenset({c, d})})
        self.assertEqual(get_binding_set(e.output_bindings), {frozenset({f}), frozenset({g}), frozenset({h})})

        self.assertEqual(get_binding_set(f.input_bindings), {frozenset({e})})
        self.assertEqual(get_binding_set(f.output_bindings), {frozenset({b, d}), frozenset({c, d})})

        self.assertEqual(get_binding_set(g.input_bindings), {frozenset({e})})
        self.assertEqual(get_binding_set(g.output_bindings), {frozenset({z})})

        self.assertEqual(get_binding_set(h.input_bindings), {frozenset({e})})
        self.assertEqual(get_binding_set(h.output_bindings), {frozenset({z})})

        self.assertEqual(get_binding_set(z.input_bindings), {frozenset({g}), frozenset({h})})


if __name__ == '__main__':
    unittest.main()