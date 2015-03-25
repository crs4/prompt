from pymine.mining.process.network import Node, Network, LabeledObject, UnexpectedEvent
from pymine.mining.process.network.graph import graph_factory
import logging
logger = logging.getLogger('cnet')
from collections import defaultdict

GRAPH_IMPL = 'nx'


class Obligation(object):
    def __init__(self, source_binding, node):
        self.source_binding = source_binding
        self.source_node = source_binding.node if source_binding else None
        self.node = node

    def __repr__(self):
        return "< Obligation node %s, binding %s >" % (self.node, self.source_binding)

# class Obligation(object):
#     def __init__(self, source_node, node):
#         self.source_node = source_node
#         self.node = node
#
#     def __repr__(self):
#         return "< Obligation node %s, source_node %s >" % (self.node, self.source_node)


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
        return str([n.label for n in self.node_set]) + "->" + self.node.label + ' freq: %s' % self.frequency


class OutputBinding(Binding):
    def __str__(self):
        return self.node.label + "->" + str([n.label for n in self.node_set]) + ' freq: %s' % self.frequency


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
        self._pending_output_bindings = []
        self._previous_events = []
        self.current_node = None
        self._xor_bindings = {}
        initial_nodes = self.get_initial_nodes()
        self._obligations = [Obligation(None, initial_nodes[0])] if initial_nodes else []

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
            input_binding_completed = self._get_input_binding_completed(obl.node)
            logger.debug("input_binding_completed for node %s: %s", obl.node, input_binding_completed)
            if input_binding_completed:
                available_nodes.add(obl.node)

        logger.debug('available_nodes %s', available_nodes)
        return available_nodes

    def _get_input_binding_completed(self, node):
        # logger.debug('_get_input_binding_completed %s', node)

        completed_bindings = defaultdict(list)
        for ib in node.input_bindings:

            logger.debug('self.events_played %s', self.events_played)
            node_set_as_labels = ib.node_set_labels()
            logger.debug('node_set_as_labels %s', node_set_as_labels)

            if node_set_as_labels <= set(self.events_played):
                indexes = [self.events_played.index(n) for n in node_set_as_labels]
                completed_bindings[max(indexes)].append(ib)

        logger.debug('completed_bindings %s', completed_bindings)
        if completed_bindings:
            max_index = max(completed_bindings.keys())
            if len(completed_bindings[max_index]) == 1:
                return completed_bindings[max_index][0]
            else:
                return max(completed_bindings[max_index])


        #
        #
        # input_binding_completed = None
        # if node.input_bindings:
        #     previous_events = set(self._previous_events)
        #     # FIXME events should be removed once consumed, otherwise it does not work with loops
        #     max_arg = [b.node_set_labels() & previous_events for b in node.input_bindings]
        #     logger.debug('max_arg %s', max_arg)
        #     if max_arg:
        #         input_binding_completed = max(max_arg)
        #         if input_binding_completed:
        #             # if remove_event:
        #             #     for e in input_binding_completed:
        #             #         self._previous_events.remove(e)
        #             input_binding_completed = set([self.get_node_by_label(l) for l in input_binding_completed])
        #             input_binding_completed = node.get_input_bindings_with(input_binding_completed, True)
        #
        #
        # return input_binding_completed

    def _find_obligations(self, node=None, source_node=None, source_binding=None):
        if node and source_node is None and source_binding is None:
            return [obl for obl in self._obligations if node == obl.node]

        elif node and source_node and source_binding is None:
            return [obl for obl in self._obligations if node == obl.node and source_node == obl.source_node]

        elif node and source_node and source_binding:
            return [obl for obl in self._obligations if node == obl.node and
                    source_node == obl.source_node and source_binding == obl.source_binding]

        elif node and source_node is None and source_binding:
            return [obl for obl in self._obligations if node == obl.node and source_binding == obl.source_binding]

        elif node is None and source_node and source_binding is None:
            return [obl for obl in self._obligations if source_node == obl.source_node]

        elif node is None and source_node is None and source_binding:
            return [obl for obl in self._obligations if source_binding == obl.source_binding]
        else:
            return []

    # def _find_obligations(self, node=None, source_node=None):
    #     if node and source_node is None:
    #         return [obl for obl in self._obligations if node == obl.node]
    #
    #     elif node and source_node:
    #         return [obl for obl in self._obligations if node == obl.node and source_node == obl.source_node]
    #
    #     elif node is None and source_node:
    #         return [obl for obl in self._obligations if source_node == obl.source_node]
    #     else:
    #         return []

    def _rm_input_obligations(self, obls):
        logger.debug('obls %s', obls)
        self._obligations.remove(obls[0])
        for n in obls[0].source_node.input_nodes:
            obls = self._find_obligations(n, obls[0].source_node)
            if obls:
                self._rm_input_obligations(obls)

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

        event_cnode = self.get_node_by_label(event)
        if event_cnode is None:
            raise UnexpectedEvent(event)
        logger.debug('event_cnode %s obligations %s', event_cnode, self._obligations)

        event_cnode.frequency += 1
        event_obls = self._find_obligations(event_cnode)


        if not event_obls:
            raise UnexpectedEvent(event)
        if event_cnode in self.get_initial_nodes():
            if event_obls:
                self._obligations.remove(event_obls[0])
            else:
                raise UnexpectedEvent(event)

        # FIXME code above can be simplified
        input_binding_completed = self._get_input_binding_completed(event_cnode)
        logger.debug('input_binding_completed %s', input_binding_completed)
        if input_binding_completed:
            input_binding_completed.frequency += 1

            for n in input_binding_completed.node_set:
                obl = self._find_obligations(event_cnode, n)
                if obl:
                    obl = obl[0]
                    logger.debug('removing  obl %s ', obl)
                    self._obligations.remove(obl)
                    obl.source_binding.frequency += 1
                    logger.debug('obl.source_node.output_bindings %s', obl.source_node.output_bindings)
                    for xor_b in obl.source_node.output_bindings:
                        logger.debug('xor_b %s', xor_b)
                        if event_cnode not in xor_b.node_set:
                            for n_to_remove in xor_b.node_set:

                                obl_ = self._find_obligations(n_to_remove, source_binding=xor_b)
                                if obl_:
                                    logger.debug('removing xor obl %s', obl_[0])
                                    self._obligations.remove(obl_[0])

            for input_node in event_cnode.input_nodes:
                pending_obls = self._find_obligations(input_node)
                if pending_obls:
                    logger.debug('removing pending_obl %s', pending_obls[0])
                    self._obligations.remove(pending_obls[0])



        self.current_node = event_cnode
        self.events_played.append(event)
        self._previous_events.append(event)

        # self._obligations += [Obligation(event_cnode, n) for n in event_cnode.output_nodes]
        logger.debug('self._obligations %s', self._obligations)

        for xor_binding in event_cnode.output_bindings:
            self._obligations += [Obligation(xor_binding, n) for n in xor_binding.node_set]
        logger.debug('self._obligations %s', self._obligations)

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

        return len(self._obligations + unexpected_events) == 0, self._obligations, unexpected_events
    
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
