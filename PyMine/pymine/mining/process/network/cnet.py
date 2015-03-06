from pymine.mining.process.network import Node, Network, LabeledObject, UnexpectedEvent
from pymine.mining.process.network.graph import graph_factory
import logging

GRAPH_IMPL = 'nx'


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
        logging.debug('input_binding_completed %s', input_binding_completed)

        bindings_completed = []
        bindings_with_node_not_completed_yet = []
        binding_to_remove = []
        obligations_to_remove = set()
        ignore_not_completed = False

        for binding, node_set in self.bindings.items():
            logging.debug('binding %s node_set %s', binding, node_set)
            logging.debug('node %s in node_set %s: %s', node, node_set, node in node_set)
            if node in node_set:
                node_set.remove(node)
                if not node_set:
                    bindings_completed.append(binding)
                else:
                    bindings_with_node_not_completed_yet.append(binding)

            elif input_binding_completed is not None and \
                    input_binding_completed.node_set == binding.node_set and not node_set:

                bindings_completed.append(binding)
                ignore_not_completed = True
            else:
                binding_to_remove.append(binding)

        ignore_not_completed = ignore_not_completed or len(bindings_with_node_not_completed_yet) == 0
        logging.debug('bindings_completed %s ignore_not_completed %s, bindings_with_node_not_completed_yet %s',
                      bindings_completed, ignore_not_completed, bindings_with_node_not_completed_yet)
        if bindings_completed and ignore_not_completed:

            # if more than one node_set is completed, let's choose the largest one
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

            # for binding in self.bindings:
            #     obligations_to_remove |= binding.node_set - self.completed_binding.node_set
        logging.debug('binding_to_remove %s', binding_to_remove)
        binding_completed_or_pending = bindings_completed + bindings_with_node_not_completed_yet
        events_pending = set()
        for b in binding_completed_or_pending:
            events_pending |= self.bindings[b]

        for binding in binding_to_remove:
            nodes_to_remove = binding.node_set - events_pending
            obligations_to_remove |= nodes_to_remove
            if nodes_to_remove == binding.node_set:
                self.bindings.pop(binding)

        return self.completed_binding or None, obligations_to_remove

    def is_completed(self):
        return self.completed_binding is not None

    def __repr__(self):
        return str(self.bindings)


class CNet(Network):
    def __init__(self, label=None):
        super(CNet,  self).__init__(label)
        self._bindings = []
        self._clean = True
        self.rewind()

    def rewind(self):
        self.events_played = []
        self.current_node = None
        self._xor_bindings = []
        initial_nodes = self.get_initial_nodes()
        self._obligations = {initial_nodes[0]} if initial_nodes else set()

    def clone(self):
        clone = CNet()
        clone.add_nodes(*[n.label for n in self.nodes])
        for binding in self.bindings:
            node = clone.get_node_by_label(binding.node.label)
            node_set = {clone.get_node_by_label(n.label) for n in binding.node_set}
            if isinstance(binding, InputBinding):
                clone.add_input_binding(node, node_set)
            else:
                clone.add_output_binding(node, node_set)
        return clone

    def reset_frequencies(self):
        logging.debug('reset')
        for node in self.nodes:
            node.frequency = 0
        for binding in self.bindings:
            binding.frequency = 0

    def shortest_path(self, start_node=None, end_node=None, max_level=30):

        class FakeNode(object):
            def __init__(self, node_):
                self.node = node_

            @property
            def output_bindings(self):
                return self.node.output_bindings

            def __str__(self):
                return self.node.label

            def __repr__(self):
                return self.node.label

        graph = graph_factory(GRAPH_IMPL)
        final_nodes = []

        def _add_node(node, level, final_nodes_, end_node_):
            logging.debug('----------------')
            logging.debug('node %s level %s', node, level)
            if level == max_level or node.node == end_node_.node:
                final_nodes_.append(node)
            else:

                for output_binding in node.output_bindings:
                    previous_node_obj = node
                    logging.debug('node.output_bindings %s', node.output_bindings)
                    for output_node in output_binding.node_set:
                        logging.debug('output_node %s', output_node)
                        output_fake_node = FakeNode(output_node)
                        graph.add_node(output_fake_node)
                        logging.debug('connecting %s -> %s', previous_node_obj, output_fake_node)
                        graph.add_edge(previous_node_obj, output_fake_node)
                        previous_node_obj = output_fake_node
                    _add_node(output_fake_node, level + 1, final_nodes_, end_node_)
                    # TODO it uses only one of the nodes in node_set, so it could fail in some cases...
            return node

        initial_node = FakeNode(start_node if start_node else self.get_initial_nodes()[0])
        graph.add_node(initial_node)
        final_node = 'end'
        graph.add_node(final_node)
        _add_node(initial_node, 0, final_nodes, FakeNode(end_node if end_node else self.get_final_nodes()[0]))
        logging.debug('final_nodes %s', final_nodes)
        for n in final_nodes:
            graph.add_edge(n, final_node)

        cost, path = graph.shortest_path(initial_node, final_node)
        cost -= 1
        path = [p.node for p in path[:-1]]
        return cost, path

    @property
    def bindings(self):
        return self._bindings

    def _add_binding(self, binding):
        self.rewind()
        self._bindings.append(binding)
        return binding

    def add_node(self, label, frequency=None, attrs=None):
        self.rewind()
        return super(CNet, self).add_node(label, frequency, attrs)

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

    @property
    def available_nodes(self):
        if self.current_node is None:
            return set(self.get_initial_nodes())
        available_nodes = set()
        logging.debug('self._obligations %s', self._obligations)
        for obl in self._obligations:
            input_binding_completed = self._get_input_binding_completed(obl)
            logging.debug("input_binding_completed for node %s: %s", obl, input_binding_completed)
            if input_binding_completed:
                available_nodes.add(obl)

        logging.debug('available_nodes %s', available_nodes)
        return available_nodes

        # available_nodes = set()
        # logging.debug('self._xor_bindings %s', self._xor_bindings)
        # logging.debug('self.current_node %s', self.current_node)
        # for xor in self._xor_bindings:
        #     for orig_bindings, binding in xor.bindings.items():
        #         for node in binding:
        #             for input_bindings in node.input_bindings:
        #                 if input_bindings.node_set <= {self.get_node_by_label(e) for e in self._events_played}:
        #                     available_nodes.add(node)
        #
        # # logging.debug('available_nodes %s', available_nodes)
        # # for node in self.current_node.output_nodes:
        # #     logging.debug('node %s', node)
        # #     for binding in node.input_bindings:
        # #         if binding.node_set <=
        # #             available_nodes.add(node)
        # logging.debug('available_nodes %s', available_nodes)
        # return available_nodes

    def _get_input_binding_completed(self, node):
        logging.debug('_get_input_binding_completed %s', node)
        input_binding_completed = None
        if node.input_bindings:
            previous_events = set(self.events_played)
            max_arg = [b.node_set_labels() & previous_events for b in node.input_bindings]
            logging.debug('max_arg %s', max_arg)
            if max_arg:
                input_binding_completed = max(max_arg)
                if input_binding_completed:
                    input_binding_completed = set([self.get_node_by_label(l) for l in input_binding_completed])
                    input_binding_completed = node.get_input_bindings_with(input_binding_completed, True)
        return input_binding_completed

    def replay_event(self, event, restart=False):
        if restart:
            self.rewind()
        if self._clean:
            self.reset_frequencies()
            self._clean = False

        self.events_played.append(event)
        event_cnode = self.get_node_by_label(event)
        if event_cnode is None:
            raise UnexpectedEvent(event)

        logging.debug('event_cnode %s obligations %s', event_cnode, self._obligations)

        logging.debug('id(event_cnode) %s. obl_ids %s ', id(event_cnode), [id(obl) for obl in self._obligations])
        logging.debug('event_cnode in self._obligations %s', event_cnode in self._obligations)
        if event_cnode in self._obligations:
            self.current_node = event_cnode
            event_cnode.frequency += 1
            logging.debug('event_cnode %s. obligations %s', event_cnode, self._obligations)
            self._obligations.remove(event_cnode)


            # incrementing input_binding_frequency
            input_binding_completed = self._get_input_binding_completed(event_cnode)
            if input_binding_completed:
                input_binding_completed.frequency += 1

            logging.debug('_xor_bindings %s', self._xor_bindings)
            logging.debug('input_binding_completed %s', input_binding_completed)
            bindings_to_remove = []
            nodes_to_remove = set()

            for xor_binding in self._xor_bindings:
                logging.debug('xor_binding %s', xor_binding)
                completed_binding, obligations_to_remove = xor_binding.remove_node(event_cnode, input_binding_completed)
                logging.debug('completed_binding %s, obligations_to_remove %s', completed_binding, obligations_to_remove)

                nodes_to_remove |= obligations_to_remove

                if completed_binding:
                    logging.debug("************xor_binding.completed_binding %s ", xor_binding.completed_binding)

                    for b in xor_binding.bindings:
                        if b != completed_binding:
                            nodes_to_remove |= b.node_set

                    # in case two bindings share one or more nodes
                    nodes_to_remove = nodes_to_remove - xor_binding.completed_binding.node_set
                    bindings_to_remove.append(xor_binding)

            logging.debug("nodes to remove from obligations %s ", nodes_to_remove)
            for node in nodes_to_remove:
                try:
                    if node != event_cnode:
                        self._obligations.remove(node)
                except KeyError:
                    logging.debug('unexpected event %s', node)
                    raise UnexpectedEvent(node.label)

            for xor_binding in bindings_to_remove:
                self._xor_bindings.remove(xor_binding)

            for xor_binding in event_cnode.output_bindings:
                self._obligations |= set([el for el in xor_binding.node_set])

            if event_cnode.output_bindings:
                self._xor_bindings.append(_XorBindings(event_cnode.output_bindings, self))

            logging.debug('obligations %s', self._obligations)
        else:
            raise UnexpectedEvent(event)

    def replay_sequence(self, sequence):
        self.rewind()
        unexpected_events = set()

        for index, event in enumerate(sequence):
            logging.debug('-------------------------------')
            logging.debug('event %s, obligations %s', event, self._obligations)
            restart = index == 0
            try:
                self.replay_event(event, restart)
            except UnexpectedEvent as ex:
                logging.debug('unexpected event %s', event)
                unexpected_events.add(ex.event)

        return len(self._obligations | set(unexpected_events)) == 0, self._obligations, unexpected_events
    
    def get_json(self):
        json = [{'label': str(self.label),
                 'nodes': [node.get_json() for node in self.nodes],
                 'arcs': [arc.get_json() for arc in self.arcs],
                 'bindings': [binding.get_json() for binding in self.bindings]}]
        return json