import unittest
from pymine.mining.process.network.cnet import CNet, CNode, InputBinding, OutputBinding
from pymine.mining.process.network.graph import PathDoesNotExist
import logging
logging.basicConfig(level=logging.DEBUG, format="%(filename)s %(lineno)s %(levelname)s: %(message)s")


def _create_cnet():
    cnet = CNet()
    a, b, c, d, e = cnet.add_nodes('a', 'b', 'c', 'd', 'e')

    cnet.add_output_binding(a, {b, c})
    cnet.add_output_binding(a, {d})

    cnet.add_input_binding(b, {a})
    cnet.add_output_binding(b, {e})

    cnet.add_input_binding(c, {a})
    cnet.add_output_binding(c, {e})

    cnet.add_input_binding(d, {a})
    cnet.add_output_binding(d, {e})

    cnet.add_input_binding(e, {b, c})
    cnet.add_input_binding(e, {d})
    return cnet, a, b, c, d, e


class CNetTestCase(unittest.TestCase):

    def test_add_node(self):
        net = CNet()
        a = net.add_node('a')
        self.assertTrue(isinstance(a, CNode))
        self.assertTrue(len(a.input_bindings) == 0)
        self.assertTrue(len(a.output_bindings) == 0)

    def test_add_node_attrs(self):
        net = CNet()
        freq = 1
        attrs = {'test': True}
        a = net.add_node('a', frequency=freq, attrs=attrs)
        self.assertTrue(a.label == 'a')
        self.assertTrue(a.frequency == freq)
        self.assertTrue(a.attrs == attrs)

    def test_add_nodes(self):
        net = CNet()
        a, b = net.add_nodes('a', 'b')
        self.assertTrue(a.label == 'a')
        self.assertTrue(b.label == 'b')
        self.assertTrue(set(net.nodes) == {a, b})

    def test_add_bindings(self):
        net = CNet()
        a, b, c, d = net.add_nodes('a', 'b', 'c', 'd')
        binding_b_c = net.add_output_binding(a, {b, c})
        binding_b = net.add_output_binding(a, {b}, 1)

        binding_b_a = net.add_input_binding(b, {a})
        binding_c_a = net.add_input_binding(c, {a})

        binding_d_b = net.add_input_binding(d, {b, c})
        binding_d_c = net.add_input_binding(d, {b})

        self.assertTrue(a.output_bindings == [binding_b_c, binding_b])
        self.assertTrue(d.input_bindings == [binding_d_b, binding_d_c])
        self.assertTrue(set(net.bindings) ==
                        {binding_b, binding_b_c, binding_d_b, binding_d_c, binding_b_a, binding_c_a})

        self.assertTrue(isinstance(binding_b_c, OutputBinding))
        self.assertTrue(isinstance(binding_d_c, InputBinding))

        self.assertTrue(binding_b_c.node == a)
        self.assertTrue(binding_b_c.node_set == {b, c})
        self.assertTrue(binding_b.frequency == 1)

        self.assertIn(binding_b_c, a.get_output_bindings_with({b}))
        self.assertIn(binding_b, a.get_output_bindings_with({b}))
        self.assertEqual(len(a.get_output_bindings_with({b})), 2)

        self.assertIn(binding_b_a, b.get_input_bindings_with({a}))
        self.assertEqual(len(b.get_input_bindings_with({a})), 1)

        self.assertEqual(len(a.output_arcs), 2)
        self.assertEqual(len(a.input_arcs), 0)

        self.assertEqual(len(b.input_arcs), 1)
        self.assertEqual(len(b.output_arcs), 1)

        self.assertEqual(len(c.input_arcs), 1)
        self.assertEqual(len(c.output_arcs), 1)

        self.assertEqual(len(d.input_arcs), 2)
        self.assertEqual(len(d.output_arcs), 0)

    def test_replay(self):
        cnet, a, b, c, d, e = _create_cnet()
        self.assertTrue(cnet.replay_sequence(['a', 'd', 'e'])[0])

        self.assertEqual(a.frequency, 1)
        self.assertEqual(b.frequency, 0)
        self.assertEqual(c.frequency, 0)
        self.assertEqual(d.frequency, 1)
        self.assertEqual(e.frequency, 1)

        self.assertEqual(a.get_output_bindings_with({d})[0].frequency, 1)
        self.assertEqual(a.get_output_bindings_with({b})[0].frequency, 0)
        self.assertEqual(a.get_output_bindings_with({c})[0].frequency, 0)
        self.assertEqual(a.get_output_bindings_with({b, c})[0].frequency, 0)

        self.assertEqual(d.get_output_bindings_with({e})[0].frequency, 1)
        self.assertEqual(d.get_input_bindings_with({a})[0].frequency, 1)
        self.assertEqual(e.get_input_bindings_with({b, c})[0].frequency, 0)
        self.assertEqual(e.get_input_bindings_with({d})[0].frequency, 1)

    def test_replay_two_bindings_with_same_node(self):
        cnet = CNet()
        a, b, c, d = cnet.add_nodes('a', 'b', 'c', 'd')

        cnet.add_output_binding(a, {b, c})
        cnet.add_output_binding(a, {b})

        cnet.add_input_binding(b, {a})
        cnet.add_output_binding(b, {d})

        cnet.add_input_binding(c, {a})
        cnet.add_output_binding(c, {d})

        cnet.add_input_binding(d, {b})
        cnet.add_input_binding(d, {b, c})

        # rep_1 = cnet.replay_sequence(['a', 'b', 'd'])
        # logging.debug('rep_1 %s', rep_1)
        # self.assertTrue(rep_1[0])
        #
        # rep_2 = cnet.replay_sequence(['a', 'b', 'c', 'd'])
        # logging.debug('rep_2 %s', rep_2)
        # self.assertTrue(rep_2[0])

        rep_3 = cnet.replay_sequence(['a', 'c', 'b', 'd'])
        logging.debug('rep_3 %s', rep_3)
        self.assertTrue(rep_3[0])

    def test_replay_two_bindings_with_same_node_2(self):
        cnet = CNet()
        a, b, c, d, e = cnet.add_nodes('a', 'b', 'c', 'd', 'e')

        cnet.add_output_binding(a, {b, c})
        cnet.add_output_binding(a, {b, d})

        cnet.add_input_binding(b, {a})
        cnet.add_output_binding(b, {e})

        cnet.add_input_binding(c, {a})
        cnet.add_output_binding(c, {e})

        cnet.add_input_binding(d, {a})
        cnet.add_output_binding(d, {e})

        cnet.add_input_binding(e, {b, c})
        cnet.add_input_binding(e, {b, d})

        self.assertTrue(cnet.replay_sequence(['a', 'b', 'd', 'e'])[0])

    def test_replay_concurrency(self):
        cnet, a, b, c, d, e = _create_cnet()
        rep_1 = cnet.replay_sequence(['a', 'b', 'c',  'e'])
        logging.debug('rep_1 %s', rep_1)
        self.assertTrue(rep_1[0])
        self.assertTrue(cnet.replay_sequence(['a', 'c', 'b',  'e'])[0])
        self.assertEqual(e.get_input_bindings_with({b, c})[0].frequency, 2)

    def test_replay_loop(self):
        cnet = CNet()
        a, b, c = cnet.add_nodes('a', 'b', 'c')

        cnet.add_output_binding(a, {b})
        cnet.add_input_binding(b, {a})
        cnet.add_input_binding(b, {b})

        cnet.add_output_binding(b, {b})
        cnet.add_output_binding(b, {c})
        cnet.add_input_binding(c, {b})

        result_1 = cnet.replay_sequence(['a', 'b', 'c'])
        logging.debug('result_1 %s', result_1)
        self.assertTrue(result_1[0])
        result_2 = cnet.replay_sequence(['a', 'b', 'b', 'c'])
        self.assertTrue(result_2[0])


    def test_replay_failing(self):
        cnet, a, b, c, d, e = _create_cnet()
        replay_result = cnet.replay_sequence(['a', 'd', 'd'])
        self.assertFalse(replay_result[0])
        self.assertEqual(set(replay_result[1]), {e})
        self.assertEqual(set(replay_result[2]), {'d'})

    def test_replay_failing_2(self):
        cnet, a, b, c, d, e = _create_cnet()
        replay_result = cnet.replay_sequence(['a', 'b', 'c', 'd', 'e'])
        self.assertFalse(replay_result[0])

    def test_replay_failing_no_initial_node(self):
        cnet, a, b, c, d, e = _create_cnet()
        replay_result = cnet.replay_sequence(['d', 'e'])
        self.assertFalse(replay_result[0])
        self.assertEqual(replay_result[1], {a})
        self.assertEqual(replay_result[2], {'d', 'e'})

    def test_replay_failing_unknown_events(self):
        cnet, a, b, c, d, e = _create_cnet()
        replay_result = cnet.replay_sequence(['a', 'd', 'y', 'e'])
        self.assertFalse(replay_result[0])
        self.assertEqual(len(replay_result[1]), 0)
        self.assertEqual(replay_result[2], set('y'))

    def test_shortest_path(self):
        cnet = CNet()
        a, b, c = cnet.add_nodes('a', 'b', 'c')

        cnet.add_output_binding(a, {b})
        cnet.add_input_binding(b, {a})
        cnet.add_output_binding(b, {c})
        cnet.add_input_binding(c, {b})

        cost, path = cnet.shortest_path()
        self.assertEqual(path, [a, b, c])
        self.assertEqual(cost, 2)

    def test_shortest_path_2(self):
        cnet, a, b, c, d, e = _create_cnet()
        cost, path = cnet.shortest_path()
        self.assertEqual(path, [a, d, e])

    def test_shortest_path_3(self):
        cnet = CNet()
        a, b, c,  e = cnet.add_nodes('a', 'b', 'c', 'e')
        cnet.add_output_binding(a, {b, c})
        cnet.add_input_binding(b, {a})
        cnet.add_output_binding(b, {e})

        cnet.add_input_binding(c, {a})
        cnet.add_output_binding(c, {e})
        cnet.add_input_binding(e, {b, c})

        cost, path = cnet.shortest_path()
        logging.debug('path %s', path)
        self.assertEqual(path[0], a)
        self.assertTrue({path[1]} <= {b, c})
        self.assertTrue({path[2]} <= {b, c})
        self.assertEqual(path[3], e)

    def test_shortest_path_loop(self):
        cnet = CNet()
        a, b, c = cnet.add_nodes('a', 'b', 'c')

        cnet.add_output_binding(a, {b})
        cnet.add_input_binding(b, {a})

        cnet.add_input_binding(b, {b})
        cnet.add_output_binding(b, {b})

        cnet.add_output_binding(b, {c})
        cnet.add_input_binding(c, {b})

        cost, path = cnet.shortest_path()
        self.assertEqual(path, [a, b, c])

    def test_shortest_path_custom_nodes(self):
        cnet, a, b, c, d, e = _create_cnet()
        cost, path = cnet.shortest_path(b, e)
        self.assertEqual(path, [b, e])

        cost, path = cnet.shortest_path(a, d)
        self.assertEqual(path, [a, d])

        self.assertRaises(PathDoesNotExist, cnet.shortest_path, b, c)

        cost, path = cnet.shortest_path(a, e)
        self.assertEqual(path, [a, d, e])

    def test_available_nodes(self):
        cnet, a, b, c, d, e = _create_cnet()
        self.assertEqual(cnet.available_nodes, {a})

        cnet.replay_event('a')
        self.assertEqual(cnet.available_nodes, {b, c, d})

        cnet.replay_event('b')
        self.assertEqual(cnet.available_nodes, {c})

        cnet.replay_event('c')
        self.assertEqual(cnet.available_nodes, {e})

        cnet.replay_event('e')
        self.assertEqual(cnet.available_nodes, set([]))

    def test_clone(self):
        net = CNet()
        a, b, c, d = net.add_nodes('a', 'b', 'c', 'd')
        net.add_output_binding(a, {b, c})
        net.add_input_binding(b, {a})
        net.add_input_binding(c, {a})

        net.add_output_binding(b, {d})
        net.add_output_binding(c, {d})
        net.add_input_binding(d, {b, c})

        clone = net.clone()
        a_clone = clone.get_node_by_label(a.label)
        b_clone = clone.get_node_by_label(b.label)
        c_clone = clone.get_node_by_label(c.label)
        d_clone = clone.get_node_by_label(d.label)
        self.assertTrue(clone.get_initial_nodes(), [a_clone])
        self.assertTrue(clone.get_final_nodes(), [d_clone])

        self.assertTrue(a_clone.output_nodes, {b_clone, c_clone, d_clone})
        self.assertTrue(a_clone.output_bindings[0].node_set, {b_clone, c_clone})

        self.assertTrue(b_clone.input_nodes, {a_clone})
        self.assertTrue(b_clone.output_nodes, {d_clone})

        self.assertTrue(c_clone.input_nodes, {a_clone})
        self.assertTrue(c_clone.output_nodes, {d_clone})

        self.assertTrue(d_clone.input_nodes, {b_clone, c_clone})
        self.assertTrue(d_clone.input_bindings[0].node_set, {b_clone, c_clone})




if __name__ == '__main__':
    unittest.main()
