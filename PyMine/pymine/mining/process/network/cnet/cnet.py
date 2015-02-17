from pymine.mining.process.network.dependency.dgraph import DependencyGraph as DependencyGraph
from collections import defaultdict
import logging
class CNet(DependencyGraph):

    def __init__(self):
        super(CNet, self).__init__()


def _convert_to_set(seq):
    if not isinstance(seq, set):
            seq = set(seq)
    return seq


class CNet2(DependencyGraph):

    def __init__(self, _id=None, nodes=None):
        self._init_id(_id)
        self._nodes = nodes or set()
        self._input_bindings = defaultdict(set)
        self._output_bindings = defaultdict(set)

    def add_nodes(self, *nodes):
        self._nodes.update(set(nodes))

    def add_input_bindings(self, node, bindings):
        bindings = _convert_to_set(bindings)
        self._input_bindings[node].update(bindings)

    def add_output_bindings(self, node, bindings):
        bindings = _convert_to_set(bindings)
        self._output_bindings[node].update(bindings)

    def get_initial_node(self):
        for node in self._nodes:
            if len(self._input_bindings[node]) == 0:
                return node

    def get_final_node(self):
        for node in self._nodes:
            if len(self._output_bindings[node]) == 0:
                return node

    def get_output_bindings(self, node):
        if node is None:
            return set(self.get_initial_node())
        return self._output_bindings[node]

    def get_input_bindings(self, node):
        return self._input_bindings[node]

    def replay_case(self, case):
        current_node = None
        initial_node = self.get_initial_node()
        obligations = defaultdict(list)
        obligations[current_node].append(initial_node)
        available_nodes = {initial_node}

        for event in case:
            logging.debug('event %s, current_node %s,  obligations %s ,available_nodes %s', event, current_node,
                          obligations, available_nodes)

            if event in available_nodes:
                obligations[current_node].remove(event)
                for binding in self.get_output_bindings(current_node):

                    #removing xor obligations
                    if event not in binding:
                        for el in binding:
                            obligations[current_node].remove(el)

                for binding in self.get_output_bindings(event):
                    obligations[event].extend([el for el in binding])

                #updating variables
                available_nodes = set()
                for obligation in obligations.values():
                    available_nodes.update(obligation)

                if len(obligations[current_node]) == 0:
                    obligations.pop(current_node)
                current_node = event
                logging.debug('obligations %s', obligations)
        logging.debug('obligations %s', obligations)
        return len(obligations) == 0, obligations


    def replay_log(self, log):
        pass
