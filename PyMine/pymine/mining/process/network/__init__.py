import uuid


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
    def __init__(self, label=None):
        self.label = label or str(uuid.uuid4())
        #self.label = label

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

        #self.net = self.input_node.net

    def get_json(self):
        json = [{'label': str(self.label),
                 'start_node': self.start_node.label,
                 'end_node': self.end_node.label,
                 'frequency': self.frequency,
                 'attributes': self.attrs}]
        return json

    '''
    def __str__(self):
        doc = "label %s %s -> %s" % (self.label, self.input_node, self.output_node)
        if self.frequency is not None:
            doc += " freq: %s" % self.frequency
        return doc
    '''

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


class Network(LabeledObject):
    def __init__(self, label=None):
        super(Network, self).__init__(label)
        self._nodes = []
        self._arcs = []

    def has_assigned_values(self):
        pass

    @property
    def nodes(self):
        return self._nodes

    @property
    def arcs(self):
        return self._arcs

    def _create_node(self, label, frequency=None, attrs={}):
        return Node(label, self, frequency, attrs)

    def add_node(self, label, frequency=None, attrs={}):
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

    def _create_arc(self, node_a, node_b, label=None, frequency=None, attrs={}, **kwargs):
        return Arc(node_a, node_b, label, frequency, attrs)

    def _add_arc(self, arc, node_a, node_b):
        self._arcs.append(arc)
        node_a.output_arcs.append(arc)
        node_b.input_arcs.append(arc)
        return arc

    def add_arc(self, node_a, node_b, label=None, frequency=None, attrs=None):
        arc = self._create_arc(node_a, node_b, label, frequency, attrs)
        return self._add_arc(arc, node_a, node_b)

    def get_json(self):
        json = [{'label': str(self.label),
                 'nodes': [node.get_json() for node in self.nodes],
                 'arcs': [arc.get_json() for arc in self.arcs]}]
        return json


def get_network_from_json(json):
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
        print("An error occurred while trying to create a Network from a json")
        print(e.message)
        return None