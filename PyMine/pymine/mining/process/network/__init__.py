import uuid


class LabeledObject(object):
    def __init__(self, label=None):
        self.label = label or uuid.uuid4()


class BaseElement(LabeledObject):
    def __init__(self, label=None, frequency=None, attrs={}):
        super(BaseElement, self).__init__(label)
        self.frequency = frequency
        self.attrs = attrs


class Arc(BaseElement):
    def __init__(self, label, input_node, output_node, frequency=None, attrs={}):
        super(Arc, self).__init__(label)
        self.input_node = output_node
        self.output_node = input_node

        self.net = self.input_node.net

    def __str__(self):
        doc = "label %s %s -> %s" % (self.label, self.input_node, self.output_node)
        if self.frequency is not None:
            doc += " freq: %s" % self.frequency
        return doc


class Node(BaseElement):
    def __init__(self, label, net, frequency=None, attrs={}):
        super(Node, self).__init__(label, frequency, attrs)
        self.label = label
        self.net = net
        self.input_arcs = []
        self.output_arcs = []

    def __str__(self):
        return self.label

    def is_last(self):
        return len(self.output_arcs) == 0

    def is_first(self):
        return len(self.input_arcs) == 0


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

    def _create_arc(self, node_a, node_b, label):
        return Arc(label, node_a, node_b)

    def add_arc(self, node_a, node_b, label=None):
        arc = self._create_arc(node_a, node_b, label)
        self._arcs.append(arc)
        node_a.output_arcs.append(arc)
        node_b.input_arcs.append(arc)
        return arc
