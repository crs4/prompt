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
    def __repr__(self):
        return str(self)

    def node_set_labels(self):
        return set(n.label for n in self.node_set)


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

    def _get_bindings_with(self, node_set, exactly, type_):
        bindings = self.input_bindings if type_ == 'input' else self.output_bindings
        if exactly:
            for binding in bindings:
                if binding.node_set == node_set:
                    return binding

        else:
            return [binding for binding in bindings if node_set <= binding.node_set]

    def get_input_bindings_with(self, node_set, exactly=False):
        return self._get_bindings_with(node_set, exactly, 'input')

    def get_output_bindings_with(self, node_set, exactly=False):
        return self._get_bindings_with(node_set, exactly, 'output')


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
        self.bindings = {}
        self.nodes = set()
        self.completed_binding = None
        for binding in bindings:
            self.nodes |= binding.node_set
            tmp_bindings = set(binding.node_set)
            if not final_node in tmp_bindings:
                tmp_bindings.add(final_node)
            self.bindings[binding] = tmp_bindings

    def has_node(self, node):
        for binding in self.bindings.values():
            if node in binding:
                return True
        return False

    def remove_node(self, node):
        logging.debug('self.has_node(%s) %s', node,  self.has_node(node))
        if self.has_node(node):
            bindings_completed = []
            for orig_binding, binding in self.bindings.items():
                if node in binding:
                    binding.remove(node)
                    if not binding:
                        bindings_completed.append(orig_binding)
                        # orig_binding.frequency += 1
                        # self.completed_binding = orig_binding
                        # logging.debug('self.completed_binding %s', self.completed_binding)
                        # break
            if bindings_completed:
                # if more than one binding is completed, let's choose the largest one
                # {b,c} when bindings_completed == [{b}, {b,c}] for example
                max_one = max(bindings_completed, key=lambda b: len(b.node_set))
                max_one.frequency += 1
                self.completed_binding = max_one
                logging.debug('self.completed_binding %s', self.completed_binding)


    def is_completed(self):
        return self.completed_binding is not None

    def __repr__(self):
        return str(self.bindings)


class CNet(Network):
    def __init__(self, label=None):
        super(CNet,  self).__init__(label)
        self._bindings = []
        self._clean = True

    def reset(self):
        for node in self.nodes:
            node.frequency = 0
        for binding in self.bindings:
            binding.frequency = 0

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
        if self._clean:
            self.reset()
            self._clean = False

        initial_node = self.get_initial_nodes()[0]
        obligations = {initial_node}
        unexpected_events = set()
        bindings = []

        for index, event in enumerate(sequence):
            logging.debug('-------------------------------')
            logging.debug('event %s, obligations %s', event, obligations)

            event_cnode = self.get_node_by_label(event)
            if event_cnode is None:
                unexpected_events.add(event)
                continue

            event_cnode.frequency += 1
            if event_cnode in obligations:
                logging.debug('event_cnode %s. obligations %s', event_cnode, obligations)
                obligations.remove(event_cnode)
                if event_cnode.output_bindings:
                    bindings.append(_XorBindings(event_cnode.output_bindings, self.get_final_nodes()[0]))

                logging.debug('bindings %s', bindings)
                bindings_to_remove = []
                for xor_binding in bindings:
                    logging.debug('xor_binding %s', xor_binding)
                    xor_binding.remove_node(event_cnode)
                    if xor_binding.is_completed():
                        logging.debug("************xor_binding.completed_binding %s ", xor_binding.completed_binding)

                        nodes_to_remove = set()
                        for b in xor_binding.bindings:
                            if b != xor_binding.completed_binding:
                                nodes_to_remove |= b.node_set

                        # in case two bindings share one or more nodes
                        nodes_to_remove = nodes_to_remove - xor_binding.completed_binding.node_set
                        logging.debug("nodes to remove from obligations %s ", nodes_to_remove)
                        for node in nodes_to_remove:
                            try:
                                if node != event_cnode:
                                    obligations.remove(node)
                            except KeyError:
                                unexpected_events.add(node.label)

                        bindings_to_remove.append(xor_binding)

                for xor_binding in bindings_to_remove:
                    bindings.remove(xor_binding)

                for xor_binding in event_cnode.output_bindings:
                    obligations |= set([el for el in xor_binding.node_set])

                # incrementing input_binding_frequency
                if event_cnode.input_bindings:
                    previous_events = set(sequence[0:index])
                    max_arg = [b.node_set_labels() & previous_events for b in event_cnode.input_bindings]
                    logging.debug('max_arg %s', max_arg)
                    if max_arg:
                        best_binding = max(max_arg)
                        if best_binding:
                            event_cnode.get_input_bindings_with(
                                set([self.get_node_by_label(l) for l in best_binding]))[0].frequency += 1

                logging.debug('obligations %s', obligations)
            else:
                unexpected_events.add(event)
        logging.debug('obligations %s', obligations)
        logging.debug('unexpected_events %s', unexpected_events)
        return len(obligations | set(unexpected_events)) == 0, obligations, unexpected_events    
    def get_json(self):
        json = [{'label': str(self.label),
                 'nodes': [node.get_json() for node in self.nodes],
                 'arcs': [arc.get_json() for arc in self.arcs],
                 'bindings': [binding.get_json() for binding in self.bindings]}]
        return json
