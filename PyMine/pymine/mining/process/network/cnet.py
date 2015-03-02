from pymine.mining.process.network import Node, Network, LabeledObject
import logging


class UnexpectedEvent(Exception):
    def __init__(self, event, *args, **kwargs):
        super(UnexpectedEvent, self).__init__(*args, **kwargs)
        self.event = event

    @property
    def message(self):
        return "unexpected event %s" % self.event

    def __str__(self):
        return self.message


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
    def __init__(self, bindings, net):
        self.bindings = {}
        self.nodes = set()
        self.net = net
        self.completed_binding = None
        for binding in bindings:
            self.nodes |= binding.node_set
            tmp_bindings = set(binding.node_set)
            self.bindings[binding] = tmp_bindings

    def has_node(self, node):
        for binding in self.bindings.values():
            if node in binding:
                return True
        return False

    def remove_node(self, node, input_binding_completed):
        logging.debug('node %s', node)

        bindings_completed = []
        bindings_with_node_not_completed_yet = []
        for orig_binding, binding in self.bindings.items():
            logging.debug('orig_binding %s binding %s', orig_binding, binding)
            if node in binding:
                binding.remove(node)
                logging.debug('binding %s', binding)
                if binding:
                    bindings_with_node_not_completed_yet.append(node)

        logging.debug('bindings_with_node_not_completed_yet %s', bindings_with_node_not_completed_yet)
        for orig_binding, binding in self.bindings.items():
            logging.debug('orig_binding.node_set %s', orig_binding.node_set)
            logging.debug('input_binding_completed %s', input_binding_completed)

            if not binding:
                if input_binding_completed == orig_binding.node_set or node == self.net.get_final_nodes()[0] \
                        or not bindings_with_node_not_completed_yet:
                    bindings_completed.append(orig_binding)

        if bindings_completed:
            # if more than one binding is completed, let's choose the largest one
            # {b,c} when bindings_completed == [{b}, {b,c}] for example
            max_one = max(bindings_completed, key=lambda b: len(b.node_set))
            # if max_one.frequency is None:
            #     # TODO maybe it is better to set frequency to 0
            #     max_one.frequency = 1
            # else:
            #     max_one.frequency += 1
            logging.debug('bindings_completed %s', bindings_completed)
            max_one.frequency += 1

            self.completed_binding = max_one
            logging.debug('******self.completed_binding %s', self.completed_binding)

    def is_completed(self):
        return self.completed_binding is not None

    def __repr__(self):
        return str(self.bindings)


class CNet(Network):
    def __init__(self, label=None):
        super(CNet,  self).__init__(label)
        self._bindings = []
        self._clean = True
        self._init()

    def reset(self):
        logging.debug('reset')
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

    def _create_node(self, label, frequency=None, attrs=None):
        return CNode(label, self, frequency, attrs)

    def _init(self):
        self._events_played = []
        self.current_node = None
        self._xor_bindings = []
        initial_nodes = self.get_initial_nodes()
        self._obligations = {initial_nodes[0]} if initial_nodes else set()

    @property
    def available_nodes(self):
        if self.current_node is None:
            return set(self.get_initial_nodes())

        available_nodes = set()
        for b in self._xor_bindings:
            available_nodes |= b.nodes
        available_nodes |= self.current_node.output_nodes
        return available_nodes

    def replay_event(self, event, restart=False):
        if restart:
            self._init()
        if self._clean:
            self.reset()
            self._clean = False

        self._events_played.append(event)
        event_cnode = self.get_node_by_label(event)
        if event_cnode is None:
            raise UnexpectedEvent(event)

        self.current_node = event_cnode
        event_cnode.frequency += 1
        if event_cnode in self._obligations:
            logging.debug('event_cnode %s. obligations %s', event_cnode, self._obligations)
            self._obligations.remove(event_cnode)
            if event_cnode.output_bindings:
                self._xor_bindings.append(_XorBindings(event_cnode.output_bindings, self))

            # incrementing input_binding_frequency
            input_binding_completed = None
            if event_cnode.input_bindings:
                previous_events = set(self._events_played)
                max_arg = [b.node_set_labels() & previous_events for b in event_cnode.input_bindings]
                logging.debug('max_arg %s', max_arg)
                if max_arg:
                    input_binding_completed = max(max_arg)
                    if input_binding_completed:
                        input_binding_completed = set([self.get_node_by_label(l) for l in input_binding_completed])
                        event_cnode.get_input_bindings_with(input_binding_completed)[0].frequency += 1

            logging.debug('_xor_bindings %s', self._xor_bindings)
            logging.debug('input_binding_completed %s', input_binding_completed)
            bindings_to_remove = []
            for xor_binding in self._xor_bindings:
                logging.debug('xor_binding %s', xor_binding)
                xor_binding.remove_node(event_cnode, input_binding_completed)
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
                                self._obligations.remove(node)
                        except KeyError:
                            raise UnexpectedEvent(node.label)

                    bindings_to_remove.append(xor_binding)

            for xor_binding in bindings_to_remove:
                self._xor_bindings.remove(xor_binding)

            for xor_binding in event_cnode.output_bindings:
                self._obligations |= set([el for el in xor_binding.node_set])

            logging.debug('obligations %s', self._obligations)
        else:
            raise UnexpectedEvent(event)

    def replay_sequence(self, sequence):
        self._init()
        unexpected_events = set()

        for index, event in enumerate(sequence):
            logging.debug('-------------------------------')
            logging.debug('event %s, obligations %s', event, self._obligations)
            restart = index == 0
            try:
                self.replay_event(event, restart)
            except UnexpectedEvent as ex:
                unexpected_events.add(ex.event)

        return len(self._obligations | set(unexpected_events)) == 0, self._obligations, unexpected_events
    
    def get_json(self):
        json = [{'label': str(self.label),
                 'nodes': [node.get_json() for node in self.nodes],
                 'arcs': [arc.get_json() for arc in self.arcs],
                 'bindings': [binding.get_json() for binding in self.bindings]}]
        return json