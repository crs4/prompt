from pymine.mining.process.network import Node, Network, LabeledObject, UnexpectedEvent
from pymine.mining.process.network.graph import graph_factory
from pymine.mining.process.network.dependency import DependencyGraph
from collections import defaultdict
import logging
logger = logging.getLogger('cnet')

GRAPH_IMPL = 'nx'


class Obligation(object):
    def __init__(self, source_binding, node):
        self.source_binding = source_binding
        self.source_node = source_binding.node if source_binding else None
        self.node = node

    def __repr__(self):
        return "< Obligation node %s, binding %s >" % (self.node, self.source_binding)

    def __hash__(self):
        return hash(self.node.label) + hash(self.source_node.label) + hash(frozenset(self.source_binding.node_set))


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

    def has_shared_output_bindings(self, node):
        shared_b =[b for b in self.output_bindings if node in b.node_set]
        return len(shared_b) > 1

    def get_json(self):
        json = [{'label': str(self.label),
                 'input_arcs': [arc.label for arc in self.input_arcs],
                 'output_arcs': [arc.label for arc in self.output_arcs],
                 'frequency': self.frequency,
                 'attributes': self.attrs,
                 'input_bindings': [binding.label for binding in self.input_bindings],
                 'output_bindings': [binding.label for binding in self.output_bindings]}]
        return json


class CNet(DependencyGraph):
    fake_start_label = '_start'
    fake_end_label = '_end'

    def __init__(self, label=None):
        super(CNet,  self).__init__(label)
        self._input_bindings = []
        self._output_bindings = []
        self._bindings = []
        self._clean = True
        self.rewind()
        self.has_fake_start = self.has_fake_end = False

    def rewind(self):
        """
        Clear current net state: obligations, current node.
        """
        self.events_played = []
        self._input_bindings_completed_index = defaultdict(list)
        self._pending_splits = []
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
        self.rewind()
        self._input_bindings.append(binding)
        return binding

    def _add_output_binding(self, binding):
        self.rewind()
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

    def _get_input_binding_completed(self, node, consume_binding=False):
        # logger.debug('_get_input_binding_completed %s', node)

        completed_bindings = defaultdict(list)
        for ib in node.input_bindings:

            logger.debug('self.events_played %s', self.events_played)
            node_set_as_labels = ib.node_set_labels()
            logger.debug('node_set_as_labels %s', node_set_as_labels)

            pending_obl = False
            for n in ib.node_set:
                if self._find_obligations(node, n):
                    pending_obl = True
                    logger.debug('pending obl for node %s, n %s', node, n)
                    break

            if pending_obl and node_set_as_labels <= set(self.events_played):
                indexes = []
                for n in node_set_as_labels:
                    for idx, e in enumerate(self.events_played):
                        if n == e and idx not in self._input_bindings_completed_index[node]:
                            indexes.append(idx)

                # indexes = [self.events_played.index(n) for n in node_set_as_labels
                #            if self.events_played.index(n) not in self._input_bindings_completed_index[node]]
                #
                logger.debug('self._input_bindings_completed_index %s', self._input_bindings_completed_index)
                logger.debug('indexes %s', indexes)

                if indexes:
                    completed_bindings[min(indexes)].append(ib)

        logger.debug('completed_bindings %s', completed_bindings)
        logger.debug('self._input_bindings_completed_index %s', self._input_bindings_completed_index)

        # if consume_binding:
        #     for i in self._input_bindings_completed_index[node]:  # removing previous completed binding
        #             if i in completed_bindings:
        #                 completed_bindings.pop(i)
        if completed_bindings:

            min_index = min(completed_bindings.keys())
            if consume_binding:
                self._input_bindings_completed_index[node].append(min_index)

            if len(completed_bindings[min_index]) == 1:
                binding = completed_bindings[min_index][0]
            else:
                binding = max(completed_bindings[min_index], key=lambda x: len(x.node_set))

            return binding

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

        elif node is None and source_node and source_binding:
            return [obl for obl in self._obligations if source_node == obl.source_node and
                    source_binding == obl.source_binding]

        elif node is None and source_node is None and source_binding:
            return [obl for obl in self._obligations if source_binding == obl.source_binding]
        else:
            return []

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

        input_binding_completed = self._get_input_binding_completed(event_cnode, True)
        logger.debug('input_binding_completed %s', input_binding_completed)
        if input_binding_completed:
            input_binding_completed.frequency += 1

            for input_node in input_binding_completed.node_set:
                if input_node.has_shared_output_bindings(event_cnode):
                    if input_node not in self._pending_splits:
                        self._pending_splits.append(input_node)
                    shared = True
                else:
                    shared = False

                for binding in input_node.output_bindings:
                    for node in binding.node_set:
                        obls = self._find_obligations(node, input_node, binding)
                        if obls:
                            obl = obls[0]
                            if event_cnode in binding.node_set:
                                if node == event_cnode:
                                    if not shared:
                                        obl.source_binding.frequency += 1
                                    logger.debug('removing obl %s', obl)
                                    self._obligations.remove(obl)

                            else:
                                logger.debug('removing xor obl %s', obl)
                                self._obligations.remove(obl)

            if event_cnode.is_join() and self._pending_splits:
                split_node = self._pending_splits.pop()
                logger.debug('removing pending obls split %s', split_node)

                max_index = max([i for i, j in enumerate(self.events_played) if j == split_node.label])
                candidate_bindings = [ib for ib in split_node.output_bindings
                                      if ib.node_set_labels() <= set(self.events_played[max_index + 1:])]
                logger.debug('candidate_bindings %s', candidate_bindings)
                if candidate_bindings:
                    binding_completed = max(candidate_bindings, key=lambda x: len(x.node_set))
                    logger.debug('binding_completed %s', binding_completed)
                    binding_completed.frequency += 1
                    for b in split_node.output_bindings:
                        if b != binding_completed:
                            for n in b.node_set:
                                if n.label not in self.events_played[max_index:]:
                                    obls = self._find_obligations(n, source_binding=b)
                                    if obls:
                                        logger.debug('removing pending obl %s', obls[0])
                                        self._obligations.remove(obls[0])

            # for input_node in event_cnode.input_nodes:
            #     logger.debug('input_node %s', input_node)
            #     pending_obls = self._find_obligations(input_node)
            #     if pending_obls:
            #         logger.debug('removing pending_obl %s', pending_obls[0])
            #         self._obligations.remove(pending_obls[0])

        self.current_node = event_cnode
        self.events_played.append(event)

        # self._obligations += [Obligation(event_cnode, input_node) for input_node in event_cnode.output_nodes]
        logger.debug('self._obligations %s', self._obligations)

        for xor_binding in event_cnode.output_bindings:
            self._obligations += [Obligation(xor_binding, input_node) for input_node in xor_binding.node_set]
        logger.debug('self._obligations %s', self._obligations)

    def replay_sequence(self, sequence):
        """
        :param sequence: a list of events to replay
        :return: a tuple containing: a boolean telling if the replay has been completed successfully,
            a list of obligations and a list of unexpected events.
        """
        self.rewind()
        unexpected_events = []
        if self.has_fake_start:
            sequence.insert(0, self.fake_start_label)
        if self.has_fake_end:
            sequence.append(self.fake_end_label)

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

    def remove_node_from_binding(self, node, binding):
        logger.debug('remove_node_from_binding  node %s, binding %s', node, binding)
        try:
            binding.node_set.remove(node)
        except KeyError:
            logger.warning('node %s not in binding %s', node, binding)
            return

        duplicated_binding = False
        source_node = binding.node
        node_bindings = source_node.input_bindings if isinstance(binding, InputBinding) else source_node.output_bindings

        for b in node_bindings:
            if b != binding and b.node_set == binding.node_set:
                duplicated_binding = True
                break

        logger.debug('binding.node_set %s, duplicated_binding %s', binding.node_set, duplicated_binding)
        if len(binding.node_set) == 0 or duplicated_binding:
            self.remove_binding(binding)

    def remove_binding(self, binding):
        logger.debug('removing binding %s', binding)
        node = binding.node
        if isinstance(binding, InputBinding):
            self._input_bindings.remove(binding)
            node.input_bindings.remove(binding)
            for input_node in node.input_nodes:
                if not node.get_input_bindings_with({input_node}):
                    for b in input_node.get_output_bindings_with({node}):
                        b.node_set.remove(node)
                        if not b.node_set:
                            logger.debug('removing empty binding %s', b)
                            self.remove_binding(b)
                    arc_to_rm = self.get_arc_by_nodes(input_node, node)
                    if arc_to_rm:
                        logger.debug('arc_to_rm %s', arc_to_rm)
                        self.remove_arc(arc_to_rm)
        else:
            self._output_bindings.remove(binding)
            node.output_bindings.remove(binding)

            for output_node in node.output_nodes:
                if not node.get_output_bindings_with({output_node}):
                    for b in output_node.get_input_bindings_with({node}):
                        b.node_set.remove(node)
                        if not b.node_set:
                            logger.debug('removing empty binding %s', b)
                            self.remove_binding(b)
                    arc_to_rm = self.get_arc_by_nodes(node, output_node)
                    if arc_to_rm:
                        logger.debug('arc_to_rm %s', arc_to_rm)
                        self.remove_arc(arc_to_rm)

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
