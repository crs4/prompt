import uuid


class BaseElement(object):
    def __init__(self, label=None):
        self.label = label or uuid.uuid4()


class Arc(BaseElement):
    def __init__(self, label, input_node, output_node, frequency=None):
        super(Arc, self).__init__(label)
        self.input_node = input_node
        self.output_node = output_node
        self.frequency = frequency
        self.net = self.input_node.net

    def __str__(self):
        doc = "label="+self.label+" " \
            "input_node="+str(self.input_node)+" " \
            "output_node="+str(self.output_node)+" " \
            "frequency="+str(self.frequency)
        return doc


class Node(BaseElement):
    def __init__(self, label, net, frequency=None):
        super(Node, self).__init__(label)
        self.label = label
        self.net = net
        self.input_arcs = set()
        self.output_arcs = set()
        self.frequency = frequency

    def is_last(self):
        return len(self.output_arcs) == 0

    def is_first(self):
        return len(self.input_arcs) == 0


class Network(BaseElement):
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

    def add_nodes(self, *labels):
        nodes = [Node(label, self) for label in labels]
        self._nodes.extend(nodes)
        return nodes if len(nodes) else nodes[0]

    def get_initial_node(self):
        for node in self.nodes:
            if node.is_first():
                return node

    def get_final_node(self):
        for node in self.nodes:
            if node.is_last():
                return node

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

    def add_arc(self, node_a, node_b, label):
        arc = self._create_arc( node_a, node_b, label)
        node_a.output_arcs.add(arc)
        node_b.input_arcs.add(arc)
        return arc

    # def remove_arc(self, arc):
    #     node_a.output_arcs.add(arc)
    #     node_b.input_arcs.add(arc)
    #     self._arcs.add(arc)
