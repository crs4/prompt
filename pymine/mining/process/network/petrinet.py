from pymine.mining.process.network import Network, Arc, Node


class Transition(Node):

    def __str__(self):
        doc = "name="+self.label+" " \
        "input_arcs="+str(self.input_arcs)+" " \
        "output_arcs="+str(self.output_arcs)
        return doc


class Place(Node):

    def __str__(self):
        doc = "name="+self.label+" " \
        "input_arcs="+str(self.input_arcs)+" " \
        "output_arcs="+str(self.output_arcs)
        return doc


class PetriArc(Arc):

    def __str__(self):
        doc = "name="+self.label+" " \
        "start_node="+str(self.start_node)+" " \
        "end_node="+str(self.end_node)
        return doc


class Marking(object):

    def __init__(self):
        # Dictionaty describing the number of tokens in each place.
        # The key is the place while the value represents the number of tokens in the place
        self.places = {}


class PetriNet(Network):

    def __init__(self):
        self._places = {}
        self._transitions = {}
        self._arcs = {}
        self.initial_place_id = None

        # Dictionaty describing the number of tokens in each place.
        # The key is the place while the value represents the number of tokens in the place
        self.initial_marking = {}
        self.current_marking = {}

    def reset_states(self):
        self.current_marking = self.initial_marking

    def fire_transition(self, transition):
        can_fire = False
        if transition in self.transitions:
            temporary_marking = self.current_marking.copy()
            print(temporary_marking)
            for arc in self.transitions[transition].input_arcs:
                input_node = self.arcs[arc].input_node
                if (input_node in temporary_marking) and (temporary_marking[input_node] > 0):
                    temporary_marking[input_node] -= 1
                    can_fire = True
                else:
                    can_fire = False
            if can_fire:
                for arc in self.transitions[transition].output_arcs:
                    output_node = self.arcs[arc].output_node
                    temporary_marking[output_node] += 1
                self.current_marking = temporary_marking.copy()
                return self.current_marking
            else:
                return None
        else:
            return None

    def get_next_possible_markings(self):
        """
        Starting from the current marking return the list of all possible markings
        :return:
        """
        markings = []
        return markings