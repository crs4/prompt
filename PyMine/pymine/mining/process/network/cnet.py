from pymine.mining.process.network import Node, Network


class Binding(object):
    def __init__(self, node, node_set, frequency=None):
        self.node = node
        self.node_set = node_set
        self.frequency = frequency
        self.net = self.node.net


class InputBinding(Binding):
    pass


class OutputBinding(Binding):
    pass


class CNode(Node):
    def __init__(self, label, net, frequency=None, attrs={}):
        super(CNode, self).__init__(label, net, frequency, attrs)
        self.input_bindings = []
        self.output_bindings = []


class CNet(Network):
    def __init__(self, label=None):
        super(CNet,  self).__init__(label)
        self._bindings = []

    @property
    def bindings(self):
        return self._bindings

    def replay_case(self, case):
        pass

    def replay_log(self, log):
        pass

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

    def _create_node(self, label, frequency=None, attrs={}):
        return CNode(label, self, frequency, attrs)
