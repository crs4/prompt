from pymine.mining.process.network import Node, Network, LabeledObject, UnexpectedEvent
from pymine.mining.process.network.graph import graph_factory
import logging
logger = logging.getLogger('cnet')

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
        for binding in self.bindings:
            if node in binding.node_set:
                return True
        return False

    def remove_node(self, node, input_binding_completed):
        logger.debug('----------remove node-----------')
        logger.debug('node %s', node)
        logger.debug('input_binding_completed %s', input_binding_completed)

        bindings_completed = []
        bindings_with_node_not_completed_yet = []
        binding_to_remove = []
        ignore_not_completed = False
        nodes_to_remove = []
        if self.is_completed():
            return None, set()

        for binding, node_set in self.bindings.items():
            if node in node_set:
                logger.debug('node in node_set')
                node_set.remove(node)
                if not node_set:
                    bindings_completed.append(binding)
                else:
                    bindings_with_node_not_completed_yet.append(binding)

            elif input_binding_completed is not None and \
                    input_binding_completed.node_set == binding.node_set and not node_set:
                logger.debug('elif...')
                bindings_completed.append(binding)
                ignore_not_completed = True

        ignore_not_completed = ignore_not_completed or len(bindings_with_node_not_completed_yet) == 0
        logger.debug('bindings_completed %s ignore_not_completed %s, bindings_with_node_not_completed_yet %s',
                      bindings_completed, ignore_not_completed, bindings_with_node_not_completed_yet)

        if bindings_completed and ignore_not_completed:
            for b in self.bindings:
                if b not in bindings_completed:
                    binding_to_remove.append(b)

        if bindings_with_node_not_completed_yet:
            for b in self.bindings:
                for completed_or_pending in bindings_with_node_not_completed_yet:
                    if not b.node_set & completed_or_pending.node_set:
                        logger.debug('b.node_set %s, completed_or_pending.node_set %s',
                                      b.node_set, completed_or_pending.node_set)
                        binding_to_remove.append(b)

        for b in binding_to_remove:
            if b in self.bindings:
                nodes_to_remove += list(self.bindings.pop(b))

        if bindings_completed and ignore_not_completed:

            # if more than one node_set is completed, let's choose the largest one
            # {b,c} when bindings_completed == [{b}, {b,c}] for example
            max_one = max(bindings_completed, key=lambda b: len(b.node_set))
            # if max_one.frequency is None:
            #     # TODO maybe it is better to set frequency to 0
            #     max_one.frequency = 1
            # else:
            #     max_one.frequency += 1
            logger.debug('bindings_completed %s', bindings_completed)
            max_one.frequency += 1

            self.completed_binding = max_one
            logger.debug('******self.completed_binding %s', self.completed_binding)

        return self.completed_binding or None, nodes_to_remove

    def is_completed(self):
        return self.completed_binding is not None

    def __repr__(self):
        return str(self.bindings)


class CNet(Network):
    def __init__(self, label=None):
        super(CNet,  self).__init__(label)
        self._input_bindings = []
        self._output_bindings = []
        self._bindings = []
        self._clean = True
        self.rewind()

    def rewind(self):
        """
        Clear current net state: obligations, current node.
        """
        self.events_played = []
        self.current_node = None
        self._xor_bindings = []
        initial_nodes = self.get_initial_nodes()
        self._obligations = {initial_nodes[0]} if initial_nodes else {}

    def clone(self):
        """
        :return: a new net with same nodes and arcs/bindings
        """
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
        for node in self.nodes:
            node.frequency = 0
        for binding in self.bindings:
            binding.frequency = 0

    def shortest_path(self, start_node=None, end_node=None, max_level=30):
        """
        :param start_node: the initial node of the path
        :param end_node: the final node of the path
        :param max_level: max level of depth, used to handle loop. Default: 30
        :return: a tuple containing the path cost and the path expressed as list of nodes
        """

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

        def _add_node(node, level, final_nodes_, end_node_, node_visited):
            if node.node in node_visited:
                return

            node_visited.add(node.node)
            logger.debug('----------------')
            logger.debug('node %s level %s', node, level)
            if level == max_level or node.node == end_node_.node:
                logger.debug('found end node: %s', node)
                final_nodes_.append(node)
            else:
                for output_binding in node.output_bindings:
                    previous_node_obj = node
                    logger.debug('node %s node.output_bindings %s', node, node.output_bindings)
                    for output_node in output_binding.node_set:
                        logger.debug('output_node %s', output_node)
                        output_fake_node = FakeNode(output_node)
                        graph.add_node(output_fake_node)
                        logger.debug('connecting %s -> %s', previous_node_obj, output_fake_node)
                        graph.add_edge(previous_node_obj, output_fake_node)
                        previous_node_obj = output_fake_node
                    _add_node(output_fake_node, level + 1, final_nodes_, end_node_, node_visited.copy())
                    # TODO it uses only one of the nodes in node_set, so it could fail in some cases...
            return node

        initial_node = FakeNode(start_node if start_node else self.get_initial_nodes()[0])
        graph.add_node(initial_node)
        final_node = 'end'
        graph.add_node(final_node)
        _add_node(initial_node, 0, final_nodes, FakeNode(end_node if end_node else self.get_final_nodes()[0]), set())
        logger.debug('final_nodes %s', final_nodes)
        for n in final_nodes:
            graph.add_edge(n, final_node)

        cost, path = graph.shortest_path(initial_node, final_node)
        cost -= 1
        path = [p.node for p in path[:-1]]
        return cost, path

    @property
    def bindings(self):
        """

        :return: all bindings of the net
        """
        return self._input_bindings + self._output_bindings

    @property
    def input_bindings(self):
        """

        :return: all input bindings of the net
        """

        return self._input_bindings

    @property
    def output_bindings(self):
        """

        :return: all the output bindings of the net
        """
        return self._output_bindings

    def _add_input_binding(self, binding):
        self._input_bindings.append(binding)
        return binding

    def _add_binding(self, binding):
        self.rewind()
        self._bindings.append(binding)
        return binding

    def _add_output_binding(self, binding):
        self._output_bindings.append(binding)
        return binding

    def add_node(self, label, frequency=None, attrs=None):
        """
        Add a node to the net

        :param label: an object that will be attached to the node (typically a string)
        :param frequency: initial frequency
        :param attrs: a dictionary with attributes associated to the node
        :return: a :class:`pymine.mining.process.network.cnet.CNode` instance
        """
        self.rewind()
        return super(CNet, self).add_node(label, frequency, attrs)

    def add_input_binding(self, node, node_set, label=None, frequency=None):
        """
        Add an input binding with the nodes in `node_set` to the given node

        :param node: a :class:`pymine.mining.process.network.cnet.CNode` instance
        :param node_set: a set of :class:`pymine.mining.process.network.cnet.CNode` instances
        :param label: a string associated to the binding
        :param frequency: initial frequency
        :return: a :class:`pymine.mining.process.network.cnet.InputBinding` instance
        """
        for n in node_set:
            if n not in node.input_nodes:
                self.add_arc(n, node)

        binding = self._add_input_binding(InputBinding(node, node_set, frequency, label=label))
        node.input_bindings.append(binding)
        return binding

    def add_output_binding(self, node, node_set, label=None, frequency=None):
        """
        Add an output binding with the nodes in `node_set` to the given node

        :param node: a :class:`pymine.mining.process.network.cnet.CNode` instance
        :param node_set: a set of :class:`pymine.mining.process.network.cnet.CNode` instances
        :param label: a string associated to the binding
        :param frequency: initial frequency
        :return: a :class:`pymine.mining.process.network.cnet.OutputBinding` instance
        """
        for n in node_set:
            if n not in node.output_nodes:
                self.add_arc(node, n)

        binding = self._add_output_binding(OutputBinding(node, node_set, frequency, label=label))
        node.output_bindings.append(binding)
        return binding

    def _create_node(self, label, frequency=None, attrs=None):
        return CNode(label, self, frequency, attrs)

    @property
    def available_nodes(self):
        """
        :return: a set of all the nodes available, given the current state of the net
        """
        if self.current_node is None:
            return set(self.get_initial_nodes())
        available_nodes = set()
        logger.debug('self._obligations %s', self._obligations)
        for obl in self._obligations:
            input_binding_completed = self._get_input_binding_completed(obl)
            logger.debug("input_binding_completed for node %s: %s", obl, input_binding_completed)
            if input_binding_completed:
                available_nodes.add(obl)

        logger.debug('available_nodes %s', available_nodes)
        return available_nodes

    def _get_input_binding_completed(self, node):
        logger.debug('_get_input_binding_completed %s', node)
        input_binding_completed = None
        if node.input_bindings:
            previous_events = set(self.events_played)
            max_arg = [b.node_set_labels() & previous_events for b in node.input_bindings]
            logger.debug('max_arg %s', max_arg)
            if max_arg:
                input_binding_completed = max(max_arg)
                if input_binding_completed:
                    input_binding_completed = set([self.get_node_by_label(l) for l in input_binding_completed])
                    input_binding_completed = node.get_input_bindings_with(input_binding_completed, True)
        return input_binding_completed

    def replay_event(self, event, restart=False):
        """
        :param event: a object corresponding to the label of any net nodes
        :param restart: set current_node to None and clear obligations
        :raises:
        """

        logger.debug('------replay event----------')
        logger.debug('event %s', event)
        logger.debug('obligations %s', self._obligations)
        if restart:
            self.rewind()
        if self._clean:
            self.reset_frequencies()
            self._clean = False

        self.events_played.append(event)
        event_cnode = self.get_node_by_label(event)
        if event_cnode is None:
            raise UnexpectedEvent(event)
        logger.debug('event_cnode %s obligations %s', event_cnode, self._obligations)
        if event_cnode in self._obligations:
            self.current_node = event_cnode
            event_cnode.frequency += 1
            self._obligations.remove(event_cnode)

            # incrementing input_binding_frequency
            input_binding_completed = self._get_input_binding_completed(event_cnode)
            if input_binding_completed:
                input_binding_completed.frequency += 1

            nodes_to_remove = []

            for xor_binding in self._xor_bindings:
                logger.debug('xor_binding %s', xor_binding)
                logger.debug('obligations %s', self._obligations)
                completed_binding, obligations_to_remove = xor_binding.remove_node(event_cnode, input_binding_completed)
                logger.debug('completed_binding %s, obligations_to_remove %s', completed_binding, obligations_to_remove)

                nodes_to_remove += obligations_to_remove

            for node in nodes_to_remove:
                try:
                    if node != event_cnode:
                        self._obligations.remove(node)
                except KeyError:
                    logger.debug('unexpected event %s', node)
                    # raise UnexpectedEvent(node.label)

            for xor_binding in event_cnode.output_bindings:
                self._obligations |= {el for el in xor_binding.node_set}

            if event_cnode.output_bindings:
                self._xor_bindings.append(_XorBindings(event_cnode.output_bindings, self))

            logger.debug('obligations %s', self._obligations)
        else:
            raise UnexpectedEvent(event)

    def replay_sequence(self, sequence):
        """
        :param sequence: a list of events to replay
        :return: a tuple containing: a boolean telling if the replay has been completed successfully,
            a list of obligations and a list of unexpected events.
        """
        self.rewind()
        unexpected_events = []

        for index, event in enumerate(sequence):
            logger.debug('-------------------------------')
            logger.debug('event %s, obligations %s', event, self._obligations)
            restart = index == 0
            try:
                self.replay_event(event, restart)
            except UnexpectedEvent as ex:
                logger.debug('unexpected event %s', event)
                unexpected_events.append(ex.event)

        return len(self._obligations | set(unexpected_events)) == 0, self._obligations, unexpected_events
    
    def get_json(self):
        """

        :return: a json representing the net
        """
        json = [{'label': str(self.label),
                 'nodes': [node.get_json() for node in self.nodes],
                 'arcs': [arc.get_json() for arc in self.arcs],
                 'input_bindings': [binding.get_json() for binding in self.input_bindings],
                 'output_bindings': [binding.get_json() for binding in self.output_bindings]}]
        return json


def get_cnet_from_json(json):
    """

    :param json: a valid json string
    :return: a :class:`pymine.mining.process.network.cnet.CNet` instance
    """
    try:
        origin = json[0]
        label = origin['label']
        the_net = CNet(label)
        for n in origin['nodes']:
            node = [n][0][0]
            if node:
                the_net.add_node(node['label'], node['frequency'], node['attributes'])
        for a in origin['arcs']:
            arc = [a][0][0]
            if arc:
                node_a = the_net.get_node_by_label(arc['start_node'])
                node_b = the_net.get_node_by_label(arc['end_node'])
                the_net.add_arc(node_a, node_b, label=arc['label'],
                                frequency=arc['frequency'], attrs=arc['attributes'])
        for b in origin['input_bindings']:
            binding = [b][0][0]
            if binding:
                node = the_net.get_node_by_label(binding['node'])
                node_set = set()
                for n in binding['node_set']:
                    node_set.add(the_net.get_node_by_label(n))
                the_net.add_input_binding(node, node_set, label=binding['label'], frequency=binding['frequency'])
        for b in origin['output_bindings']:
            binding = [b][0][0]
            if binding:
                node = the_net.get_node_by_label(binding['node'])
                node_set = set()
                for n in binding['node_set']:
                    node_set.add(the_net.get_node_by_label(n))
                the_net.add_output_binding(node, node_set, label=binding['label'], frequency=binding['frequency'])
        return the_net
    except Exception, e:
        logger.error("An error occurred while trying to create a Network from a json")
        logger.error(e.message)
