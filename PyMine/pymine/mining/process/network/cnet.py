from pymine.mining.process.network import Node, Network, LabeledObject
import logging

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
    def __str__(self):
        return str([n.label for n in self.node_set]) + "->" + self.node.label


class OutputBinding(Binding):
    def __str__(self):
        return self.node.label + "->" + str([n.label for n in self.node_set])


class CNode(Node):
    def __init__(self, label, net, frequency=None, attrs=None):
        super(CNode, self).__init__(label, net, frequency, attrs)
        self.input_bindings = []
        self.output_bindings = []

    @property
    def obligations(self):
        return self.output_nodes
    def get_json(self):
        json = [{'label': str(self.label),
                 'input_arcs': [arc.label for arc in self.input_arcs],
                 'output_arcs': [arc.label for arc in self.output_arcs],
                 'frequency': self.frequency,
                 'attributes': self.attrs,
                 'input_bindings': [binding.label for binding in self.input_bindings],
                 'output_bindings': [binding.label for binding in self.output_bindings]}]
        return json


class _XorBindings(object):
    def __init__(self, bindings, final_node):
        self.bindings = []
        self.nodes = set()
        for binding in bindings:
            self.nodes |= binding.node_set
            tmp_bindings = set(binding.node_set)
            tmp_bindings.add(final_node)
            self.bindings.append(tmp_bindings)

    def has_node(self, node):
        for binding in self.bindings:
            if node in binding:
                return True
        return False

    def remove_node(self, node):
        logging.debug('self.has_node(%s) %s', node,  self.has_node(node))
        if self.has_node(node):
            for binding in self.bindings:
                if node in binding:
                    binding.remove(node)
                    if not binding:
                        self.bindings = []
                        break

    def is_completed(self):
        return len(self.bindings) == 0

    def __repr__(self):
        return str(self.bindings)


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
        obligations = {initial_node}
        unexpected_events = []
        bindings = []

        for event in sequence:
            logging.debug('event %s, current_node %s,  obligations %s', event, current_node, obligations)

            event_cnode = self.get_node_by_label(event)
            if event_cnode is None:
                unexpected_events.append(event)
                continue

            if event_cnode in obligations:
                logging.debug('event_cnode %s. obligations %s', event_cnode, obligations)
                obligations.remove(event_cnode)
                if event_cnode.output_bindings:
                    bindings.append(_XorBindings(event_cnode.output_bindings, self.get_final_nodes()[0]))

                logging.debug('bindings %s', bindings)
                for binding in bindings:
                    binding.remove_node(event_cnode)
                    if binding.is_completed():
                        for node in binding.nodes:
                            try:
                                obligations.remove(node)
                            except KeyError:
                                pass
                        bindings.remove(binding)

                for binding in event_cnode.output_bindings:
                    obligations |= set([el for el in binding.node_set])

                current_node = event_cnode
                logging.debug('obligations %s', obligations)
            else:
                unexpected_events.append(event)
        logging.debug('obligations %s', obligations)
        logging.debug('unexpected_events %s', unexpected_events)
        return len(obligations | set(unexpected_events)) == 0, obligations, unexpected_events 
    
    def get_json(self):
        json = [{'label': str(self.label),
                 'nodes': [node.get_json() for node in self.nodes],
                 'arcs': [arc.get_json() for arc in self.arcs],
                 'bindings': [binding.get_json() for binding in self.bindings]}]
        return json
