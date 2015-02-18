from pymine.mining.process.network import Network, Arc


class DArc(Arc):

    def __init__(self, label, input_node, output_node, frequency=None, dependency=None):
        super(DArc, self).__init__(label, input_node, output_node, frequency=None)
        self.dependency = None

    def __str__(self):
        doc = super(DArc, self).__str__()
        if self.dependency is not None:
            doc += " dep: %s" % self.dependency
        return doc


class DependencyGraph(Network):
    def __init__(self):
        super(DependencyGraph, self).__init__()

    def _create_arc(self, node_a, node_b, label):
        return DArc(label, node_a, node_b)