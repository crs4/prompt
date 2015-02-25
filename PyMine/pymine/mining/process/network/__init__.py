import uuid


class LabeledObject(object):
    def __init__(self, label=None):
        self.label = label or uuid.uuid4()


class BaseElement(LabeledObject):
    def __init__(self, label=None, frequency=None, attrs=None):
        super(BaseElement, self).__init__(label)
        self.frequency = frequency
        self.attrs = attrs or {}

    def __eq__(self, other):
        if type(self) == type(other):
            try:
                assert self.attrs == other.attrs
                assert self.label == other.label
                assert self.frequency == other.frequency
            except AssertionError, e:
                return False
            return True
        else:
            return False

class Arc(BaseElement):
    def __init__(self, input_node, output_node, label=None, frequency=None, attrs=None):
        super(Arc, self).__init__(label, frequency, attrs)
        self.input_node = input_node
        self.output_node = output_node

        #self.net = self.input_node.net

    def __eq__(self, other):
        if type(self) == type(other):
            try:
                assert self.input_node == other.input_node
                assert self.output_node == other.output_node
                assert self.label == other.label
                assert self.frequency == other.frequency
                assert self.attrs == other.attrs
            except AssertionError, e:
                return False
            return True
        else:
            return False

    def __str__(self):
        doc = "label %s %s -> %s" % (self.label, self.input_node, self.output_node)
        if self.frequency is not None:
            doc += " freq: %s" % self.frequency
        return doc


class Node(BaseElement):
    def __init__(self, label, net, frequency=None, attrs=None):
        super(Node, self).__init__(label, frequency, attrs)
        self.label = label
        self.net = net
        self.input_arcs = []
        self.output_arcs = []

    def __eq__(self, other):
        if type(self) == type(other):
            try:
                assert self.label == other.label
                assert self.net == other.net
                assert self.frequency == other.frequency
                assert self.attrs == other.attrs
                assert self.input_arcs == other.input_arcs
                assert self.output_arcs == other.output_arcs
            except AssertionError, e:
                return False
            return True
        else:
            return False

    def __str__(self):
        return str(self.label)

    def is_last(self):
        return len(self.output_arcs) == 0

    def is_first(self):
        return len(self.input_arcs) == 0

    @property
    def output_nodes(self):
        return [arc.input_node for arc in self.output_arcs]

    @property
    def input_nodes(self):
        return [arc.output_node for arc in self.input_arcs]


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

    def add_arc(self, node_a, node_b, label=None, frequency=None, attrs={}):
        arc = self._create_arc(node_a, node_b, label, frequency, attrs)
        return self._add_arc(arc, node_a, node_b)

    def __eq__(self, other):
        if type(self) == type(other):
            try:
                assert self.label == other.label
                assert self._nodes == other._nodes
                assert self._arcs == other._arcs
            except AssertionError, e:
                return False
            return True
        else:
            return False