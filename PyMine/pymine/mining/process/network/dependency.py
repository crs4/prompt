from pymine.mining.process.network import Network, Arc


class DArc(Arc):

    def __init__(self, input_node, output_node, label=None, frequency=None, dependency=None, attrs={}):
        super(DArc, self).__init__(input_node, output_node, label, frequency, attrs)
        self.dependency = dependency

    def __str__(self):
        doc = super(DArc, self).__str__()
        if self.dependency is not None:
            doc += " dep: %s" % self.dependency
        return doc


class DependencyGraph(Network):
    def __init__(self, label=None):
        super(DependencyGraph, self).__init__(label)

    def add_arc(self, node_a, node_b, label=None, frequency=None, dependency=None, attrs={}):
        arc = DArc(node_a, node_b, label, frequency, dependency, attrs)
        return self._add_arc(arc, node_a, node_b)