from pymine.mining.process.eventlog.factory import create_process_log_from_list, create_log_from_file
import unittest
import os


def get_binding_set(binding):
    return {frozenset(bi.node_set) for bi in binding}


class BackendTests(object):
    def create_miner(self, log):
        raise NotImplementedError

    def __getattr__(self, item):
        return getattr(unittest.TestCase, item)

    def test_2_and_1_xor(self):
        log = create_process_log_from_list([
            ['a', 'b', 'c', 'e'],
            ['a', 'c', 'b', 'e'],
            ['a', 'd', 'e'],
        ])
        miner = self.create_miner(log)
        cnet = miner.mine()
        a, b, c, d, e = [cnet.get_node_by_label(n) for n in ['a', 'b', 'c', 'd', 'e']]
        self.assertEqual(cnet.get_initial_nodes(), [a])
        self.assertEqual(cnet.get_final_nodes(), [e])
        self.assertEqual(get_binding_set(a.output_bindings), {frozenset({b, c}), frozenset({d})})
        self.assertEqual(get_binding_set(e.input_bindings), {frozenset({b, c}), frozenset({d})})

        self.assertEqual(a.frequency, 3)
        self.assertEqual(b.frequency, 2)
        self.assertEqual(c.frequency, 2)
        self.assertEqual(d.frequency, 1)
        self.assertEqual(e.frequency, 3)

        a_b = cnet.get_arc_by_nodes(a, b)
        self.assertEqual(a_b.frequency, 1)
        self.assertEqual(a_b.dependency, 0.5)

        a_c = cnet.get_arc_by_nodes(a, c)
        self.assertEqual(a_c.frequency, 1)
        self.assertEqual(a_c.dependency, 0.5)
        a_d = cnet.get_arc_by_nodes(a, d)
        self.assertEqual(a_d.frequency, 1)
        self.assertEqual(a_d.dependency, 0.5)

        a_b_c_bin = a.get_output_bindings_with({b, c}, True)
        a_d_bin = a.get_output_bindings_with({d}, True)
        self.assertEqual(a_b_c_bin.frequency, 2)
        self.assertEqual(a_d_bin.frequency, 1)

        e_b_c_bin = e.get_input_bindings_with({b, c}, True)
        e_d_bin = e.get_input_bindings_with({d}, True)
        self.assertEqual(e_b_c_bin.frequency, 2)
        self.assertEqual(e_d_bin.frequency, 1)

    def test_triple_and(self):
        log = create_process_log_from_list([
            ['a', 'b', 'c', 'd', 'e'],
            ['a', 'b', 'd', 'c', 'e'],
            ['a', 'c', 'b', 'd', 'e'],
            ['a', 'c', 'd', 'b', 'e'],
            ['a', 'd', 'b', 'c', 'e'],
            ['a', 'd', 'c', 'b', 'e'],
        ])
        miner = self.create_miner(log)
        cnet = miner.mine()
        a, b, c, d, e = [cnet.get_node_by_label(n) for n in ['a', 'b', 'c', 'd', 'e']]
        self.assertEqual(cnet.get_initial_nodes(), [a])
        self.assertEqual(cnet.get_final_nodes(), [e])
        self.assertEqual(get_binding_set(a.output_bindings), {frozenset({b, c, d})})
        self.assertTrue(get_binding_set(e.input_bindings), {frozenset({b, c, d})})

        a_b = cnet.get_arc_by_nodes(a, b)
        self.assertEqual(a_b.frequency, 2)
        self.assertEqual(a_b.dependency, 2.0/3)

        a_b_c_d_bin = a.get_output_bindings_with({b, c, d}, True)
        self.assertEqual(a_b_c_d_bin.frequency, 6)

        e_b_c_d_bin = e.get_input_bindings_with({b, c, d}, True)
        self.assertEqual(e_b_c_d_bin.frequency, 6)

    def test_3_and_couple(self):
        log = create_process_log_from_list([
            ['a', 'b', 'c', 'd'],
            ['a', 'c', 'b', 'd'],
            ['a', 'c', 'e', 'd'],
            ['a', 'e', 'c', 'd'],
            ['a', 'b', 'e', 'd'],
            ['a', 'e', 'b', 'd'],
        ])
        miner = self.create_miner(log)
        cnet = miner.mine(bindings_thr=0.2)
        a, b, c, d, e = [cnet.get_node_by_label(n) for n in ['a', 'b', 'c', 'd', 'e']]
        self.assertEqual(cnet.get_initial_nodes(), [a])
        self.assertEqual(cnet.get_final_nodes(), [d])
        self.assertEqual(get_binding_set(a.output_bindings), {frozenset({b, c}), frozenset({b, e}), frozenset({c, e})})
        self.assertEqual(get_binding_set(d.input_bindings), {frozenset({b, c}), frozenset({b, e}), frozenset({c, e})})

    def test_2_groups_and(self):
        log = create_process_log_from_list([
            ['a', 'b1', 'b2', 'd'],
            ['a', 'b2', 'b1', 'd'],
            ['a', 'tb1', 'tb2', 'd'],
            ['a', 'tb2', 'tb1', 'd'],
            ['a', 'b1', 'tb2', 'd'],
            ['a', 'tb2', 'b1', 'd'],
            ['a', 'tb1', 'b2', 'd'],
            ['a', 'b2', 'tb1', 'd'],
        ])
        miner = self.create_miner(log)
        cnet = miner.mine(bindings_thr=0.2)
        a, b1, b2, tb1, tb2,  d = [cnet.get_node_by_label(n) for n in ['a', 'b1', 'b2', 'tb1', 'tb2', 'd']]
        self.assertEqual(cnet.get_initial_nodes(), [a])
        self.assertEqual(cnet.get_final_nodes(), [d])
        self.assertEqual(get_binding_set(a.output_bindings),
                         {frozenset({b1, b2}), frozenset({b1, tb2}), frozenset({b2, tb1}), frozenset({tb1, tb2})})
        self.assertEqual(get_binding_set(d.input_bindings),
                         {frozenset({b1, b2}), frozenset({b1, tb2}), frozenset({b2, tb1}), frozenset({tb1, tb2})})

    def test_pg_4_dataset(self):
        dataset_path = os.path.join(os.path.dirname(__file__), '../../../../../../dataset/pg_4_label_final_node.csv')
        log = create_log_from_file(dataset_path)[0]
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

    def test_self_loop(self):
        log = create_process_log_from_list([
            ['a', 'b', 'c', 'd'],
            ['a', 'b', 'b', 'c', 'd'],
            ['a', 'b', 'b', 'b', 'b', 'c', 'd'],

        ])
        miner = self.create_miner(log)
        cnet = miner.mine(bindings_thr=0.2)
        a, b, c, d = [cnet.get_node_by_label(n) for n in ['a', 'b', 'c', 'd']]
        self.assertEqual(cnet.get_initial_nodes(), [a])
        self.assertEqual(cnet.get_final_nodes(), [d])

        self.assertEqual(get_binding_set(a.output_bindings), {frozenset({b})})

        self.assertEqual(get_binding_set(b.input_bindings), {frozenset({a}), frozenset({b})})
        self.assertEqual(get_binding_set(b.output_bindings), {frozenset({c}), frozenset({b})})

        self.assertEqual(get_binding_set(c.input_bindings), {frozenset({b})})
        self.assertEqual(get_binding_set(c.output_bindings), {frozenset({d})})

        self.assertEqual(get_binding_set(d.input_bindings), {frozenset({c})})

        b_b = cnet.get_arc_by_nodes(b, b)
        self.assertEqual(b_b.frequency, 4)

        b_b_bin = b.get_input_bindings_with({b}, True)
        self.assertEqual(b_b_bin.frequency, 4)

    def test_long_distance_loop(self):
        log = create_process_log_from_list([
            ['a', 'b',  'd', 'e', 'g'],
            ['a', 'c',  'd', 'f', 'g']
        ])
        miner = self.create_miner(log)
        cnet = miner.mine(bindings_thr=0.2)
        a, b, c, d, e, f, g = [cnet.get_node_by_label(n) for n in ['a', 'b', 'c', 'd', 'e', 'f', 'g']]
        self.assertEqual(cnet.get_initial_nodes(), [a])
        self.assertEqual(cnet.get_final_nodes(), [g])

        self.assertEqual(get_binding_set(a.output_bindings), {frozenset({b}), frozenset({c})})
        self.assertEqual(get_binding_set(a.output_bindings), {frozenset({b}), frozenset({c})})

        self.assertEqual(get_binding_set(b.input_bindings), {frozenset({a})})
        self.assertEqual(get_binding_set(b.output_bindings), {frozenset({d, e})})

        self.assertEqual(get_binding_set(c.input_bindings), {frozenset({a})})
        self.assertEqual(get_binding_set(c.output_bindings), {frozenset({d, f})})

        self.assertEqual(get_binding_set(d.input_bindings), {frozenset({b}), frozenset({c})})
        self.assertEqual(get_binding_set(d.output_bindings), {frozenset({f}), frozenset({e})})

        self.assertEqual(get_binding_set(e.input_bindings), {frozenset({b, d})})
        self.assertEqual(get_binding_set(e.output_bindings), {frozenset({g})})

        self.assertEqual(get_binding_set(f.input_bindings), {frozenset({c, d})})
        self.assertEqual(get_binding_set(f.output_bindings), {frozenset({g})})

        self.assertEqual(get_binding_set(g.input_bindings), {frozenset({f}), frozenset({e})})
