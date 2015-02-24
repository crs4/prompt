from pymine.mining.process.network import Node, Network, LabeledObject


class Binding(LabeledObject):
    def __init__(self, node, node_set, frequency=None, label=None):
        super(Binding, self).__init__(label=label)
        self.node = node
        self.node_set = node_set
        self.frequency = frequency
        self.net = self.node.net

    def get_json(self):
        json = [{'label': str(self.label),
                 'node': self.node.label,
                 'node_set': [node.label for node in self.node_set],
                 'frequency': self.frequency}]
        return json

class InputBinding(Binding):
    pass


class OutputBinding(Binding):
    pass


class CNode(Node):
    def __init__(self, label, net, frequency=None, attrs=None):
        super(CNode, self).__init__(label, net, frequency, attrs)
        self.input_bindings = []
        self.output_bindings = []

    def get_json(self):
        json = [{'label': str(self.label),
                 'input_arcs': [arc.label for arc in self.input_arcs],
                 'output_arcs': [arc.label for arc in self.output_arcs],
                 'frequency': self.frequency,
                 'attributes': self.attrs,
                 'input_bindings': [binding.label for binding in self.input_bindings],
                 'output_bindings': [binding.label for binding in self.output_bindings]}]
        return json

class CNet(Network):
    def __init__(self, label=None):
        super(CNet,  self).__init__(label)
        self._bindings = []

    @property
    def bindings(self):
        return self._bindings

    def _add_binding(self, binding):
        self._bindings.append(binding)
        return binding

    def add_input_binding(self, node, node_set, frequency=None):
        for n in node_set:
            if n not in node.input_nodes:
                self.add_arc(n, node)

        binding = self._add_binding(InputBinding(node, node_set, frequency))
        node.input_bindings.append(binding)
        return binding

    def add_output_binding(self, node, node_set, frequency=None):
        for n in node_set:
            if n not in node.output_nodes:
                self.add_arc(node, n)

        binding = self._add_binding(OutputBinding(node, node_set, frequency))
        node.output_bindings.append(binding)
        return binding

    def _create_node(self, label, frequency=None, attrs={}):
        return CNode(label, self, frequency, attrs)

    def replay_sequence(self, sequence):
        current_node = None
        initial_node = self.get_initial_nodes()[0]
        obligations = [initial_node]
        available_nodes = [initial_node]
        unknown_events = []

        for event in sequence:
            logging.debug('event %s, current_node %s,  obligations %s ,available_nodes %s', event, current_node,
                          obligations, available_nodes)

            event_cnode = self.get_node_by_label(event)
            if event_cnode is None:
                unknown_events.append(event)
                continue

            if event_cnode in available_nodes:
                logging.debug('event_cnode %s. obligations %s', event_cnode, obligations)
                obligations.remove(event_cnode)
                bindings = current_node.output_bindings if current_node else []
                for binding in bindings:
                    #removing xor obligations
                    if event_cnode not in binding.node_set:
                        for el in binding.node_set:
                            obligations.remove(el)
                for binding in event_cnode.output_bindings:
                    obligations.extend([el for el in binding.node_set])

                #updating variables
                available_nodes = list(obligations)

                current_node = event_cnode
                logging.debug('obligations %s', obligations)
        logging.debug('obligations %s', obligations)
        return len(obligations + unknown_events) == 0, obligations, unknown_events        return len(obligations) == 0, obligations
    
    def get_json(self):
        json = [{'label': str(self.label),
                 'nodes': [node.get_json() for node in self.nodes],
                 'arcs': [arc.get_json() for arc in self.arcs],
                 'bindings': [binding.get_json() for binding in self.bindings]}]
        return json