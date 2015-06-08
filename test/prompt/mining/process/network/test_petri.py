import unittest, os

from prompt.mining.process.network.petrinet import PetriNet, Place, Transition, PetriArc

class TestPetri(unittest.TestCase):

    def __init__(self, label):
        super(TestPetri, self).__init__(label)

    def _create_semaphore_net(self):
        pnet = PetriNet()
        red = Place(label="red")
        green = Place(name="green")
        orange = Place(name="orange")
        r_g = Transition(name="rg")
        g_o = Transition(label="go")
        o_r = Transition(label="or")
        arc1 = PetriArc()
        arc1.input_node = red.id
        arc1.output_node = r_g.id
        arc2 = PetriArc()
        arc2.input_node = r_g.id
        arc2.output_node = green.id
        arc3 = PetriArc()
        arc3.input_node = green.id
        arc3.output_node = g_o.id
        arc4 = PetriArc()
        arc4.input_node = g_o.id
        arc4.output_node = orange.id
        arc5 = PetriArc()
        arc5.input_node = orange.id
        arc5.output_node = o_r.id
        arc6 = PetriArc()
        arc6.input_node = o_r.id
        arc6.output_node = red.id
        red.input_arcs.append(arc6.id)
        red.output_arcs.append(arc1.id)
        green.input_arcs.append(arc2.id)
        green.output_arcs.append(arc3.id)
        orange.input_arcs.append(arc4.id)
        orange.output_arcs.append(arc5.id)
        r_g.input_arcs.append(arc1.id)
        r_g.output_arcs.append(arc2.id)
        g_o.input_arcs.append(arc3.id)
        g_o.output_arcs.append(arc4.id)
        o_r.input_arcs.append(arc5.id)
        o_r.output_arcs.append(arc6.id)

        pnet.places = {red.id: red, green.id: green, orange.id: orange}
        pnet.transitions = {r_g.id: r_g, g_o.id: g_o, o_r.id: o_r}
        pnet.arcs = {arc1.id: arc1, arc2.id: arc2, arc3.id: arc3, arc4.id: arc4, arc5.id: arc5, arc6.id: arc6}
        return pnet

    # TODO refactor petrinet

    # def test_fire_transition(self):
    #     smf_net = self._create_semaphore_net()
    #     smf_net.initial_marking = {"red": 1, "green": 0, "orange": 0}
    #     smf_net.reset_states()
    #     marking = smf_net.fire_transition("rg")
    #     expected_marking = {"red": 0, "green": 1, "orange": 0}
    #     self.assertEqual(marking, expected_marking)
    #     marking = smf_net.fire_transition("go")
    #     expected_marking = {"red": 0, "green": 0, "orange": 1}
    #     self.assertEqual(marking, expected_marking)
    #     marking = smf_net.fire_transition("or")
    #     expected_marking = {"red": 1, "green": 0, "orange": 0}
    #     self.assertEqual(marking, expected_marking)
    #     marking = smf_net.fire_transition("or")
    #     expected_marking = None
    #     self.assertEqual(marking, expected_marking)
    #     marking = smf_net.fire_transition("rg")
    #     expected_marking = {"red": 0, "green": 1, "orange": 0}
    #     self.assertEqual(marking, expected_marking)
    #
    # def test_get_next_possible_markings(self):
    #     pass

def suite():
    suite = unittest.TestSuite()
    suite.addTest(TestPetri('test_fire_transition'))
    #suite.addTest(TestPetri('test_get_next_possible_markings'))
    return suite

if __name__ == '__main__':
    runner = unittest.TestPetri(verbosity=2)
    runner.run(suite())