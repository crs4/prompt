import uuid
import logging

class UnexpectedEvent(Exception):

    # TODO: move in a proper file
    def __init__(self, event, *args, **kwargs):
        super(UnexpectedEvent, self).__init__(*args, **kwargs)
        self.event = event

    @property
    def message(self):
        return "unexpected event %s" % self.event

    def __str__(self):
        return self.message


class LabeledObject(object):
    def __init__(self, label=''):
        self.label = label

    def get_json(self):
        return [{'label': str(self.label)}]

    def __eq__(self, other):
        if type(self) == type(other):
            try:
                assert self.get_json() == other.get_json()
            except AssertionError, e:
                return False
            return True
        else:
            return False

class BaseElement(LabeledObject):
    def __init__(self, label=None, frequency=None, attrs=None):
        super(BaseElement, self).__init__(label)
        self.frequency = frequency
        self.attrs = attrs or {}

    def get_json(self):
        json = [{'label': str(self.label),
                 'frequency': self.frequency,
                 'attributes': self.attrs}]
        return json

class Arc(BaseElement):
    def __init__(self, start_node, end_node, label=None, frequency=None, attrs=None):
        super(Arc, self).__init__(label, frequency, attrs)
        self.start_node = start_node
        self.end_node = end_node

    def get_json(self):
        json = [{'label': str(self.label),
                 'start_node': self.start_node.label,
                 'end_node': self.end_node.label,
                 'frequency': self.frequency,
                 'attributes': self.attrs}]
        return json

    def __str__(self):
        return self.label or "%s->%s" % (self.start_node.label, self.end_node.label)

    def __eq__(self, other):
        return self.start_node == other.start_node and self.end_node == other.end_node and self.label == other.label

    # def __hash__(self):
    #     return hash((self.start_node, self.end_node))


class Node(BaseElement):
    def __init__(self, label, net, frequency=None, attrs=None):
        super(Node, self).__init__(label, frequency, attrs)
        self.label = label
        self.net = net
        self.input_arcs = []
        self.output_arcs = []

    def get_json(self):
        json = [{'label': str(self.label),
                 'input_arcs': [arc.label for arc in self.input_arcs],
                 'output_arcs': [arc.label for arc in self.output_arcs],
                 'frequency': self.frequency,
                 'attributes': self.attrs}]
        return json

    def __str__(self):
        return str(self.label)

    def __repr__(self):
        return self.label

    def is_last(self):
        return len(self.output_arcs) == 0

    def is_first(self):
        return len(self.input_arcs) == 0

    @property
    def output_nodes(self):
        return {arc.end_node for arc in self.output_arcs}

    @property
    def input_nodes(self):
        return {arc.start_node for arc in self.input_arcs}

    def is_split(self):
        return len(self.output_nodes) > 1

    def is_join(self):
        return len(self.input_nodes) > 1

    def __eq__(self, other):
        return self.label == other.label
    #
    # def __hash__(self):
    #     return hash(self.label)
    #

class Network(LabeledObject):
    def __init__(self, label=None):
        super(Network, self).__init__(label)
        self._nodes = []
        self._arcs = []
        self.start_node = None
        self.end_node = None

    @property
    def nodes(self):
        return self._nodes

    @property
    def arcs(self):
        return self._arcs

    def _create_node(self, label, frequency=None, attrs=None):
        return Node(label, self, frequency, attrs)

    def add_node(self, label, frequency=None, attrs=None):
        node = self._create_node(label, frequency, attrs)
        self._nodes.append(node)
        return node

    def add_nodes(self, *labels):
        return [self.add_node(label) for label in labels]

    def get_initial_nodes(self):
        nodes = []
        for node in self.nodes:
            if node.is_first():
                nodes.append(node)
        return nodes

    def get_final_nodes(self):
        nodes = []
        for node in self.nodes:
            if node.is_last():
                nodes.append(node)
        return nodes

    def get_node_by_label(self, label):
        for node in self.nodes:
            if node.label == label:
                return node

    def get_arc_by_label(self, label):
        for arc in self.arcs:
            if arc.label == label:
                return arc
            
    def get_arc_by_nodes(self, input, output):
        for arc in self.arcs:
            if arc.start_node == input and arc.end_node == output:
                return arc
            
    def _create_arc(self, node_a, node_b, label=None, frequency=None, attrs=None):
        return Arc(node_a, node_b, label, frequency, attrs)

    def _add_arc(self, arc, node_a, node_b):
        self._arcs.append(arc)
        node_a.output_arcs.append(arc)
        node_b.input_arcs.append(arc)
        return arc

    def remove_arc(self, arc):
        self._arcs.remove(arc)
        arc.end_node.input_arcs.remove(arc)
        arc.start_node.output_arcs.remove(arc)

    def add_arc(self, node_a, node_b, label=None, frequency=None, attrs=None):
        existing_arc = self.get_arc_by_nodes(node_a, node_b)
        if existing_arc:
            return existing_arc
        arc = self._create_arc(node_a, node_b, label, frequency, attrs)
        return self._add_arc(arc, node_a, node_b)

    def get_json(self):
        json = [{'label': str(self.label),
                 'nodes': [node.get_json() for node in self.nodes],
                 'arcs': [arc.get_json() for arc in self.arcs]}]
        return json

    def __eq__(self, other):
        node_eq = set([n.label for n in self.nodes]) == set([n.label for n in other.nodes])
        arc_eq = set([(a.start_node.label, a.end_node.label, a.label) for a in self.arcs]) == \
                 set([(a.start_node.label, a.end_node.label, a.label) for a in other.arcs])
        return node_eq and arc_eq


def get_network_from_json(json):
    """

    :param json: a valid json string
    :return: a :class:`pymine.mining.process.network.cnet.CNet` instance
    """
    try:
        origin = json[0]
        label = origin['label']
        the_net = Network(label)
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
        return the_net
    except Exception, e:
        logging.error("An error occurred while trying to create a Network from a json")
        logging.error(e)
