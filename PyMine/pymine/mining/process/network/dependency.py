from pymine.mining.process.network import Network, Arc, Node


class DArc(Arc):

    def __init__(self, input_node, output_node, label=None, frequency=0, dependency=0.0, attrs={}):
        super(DArc, self).__init__(input_node, output_node, label, frequency, attrs)
        self.dependency = dependency

    def __str__(self):
        doc = super(DArc, self).__str__()
        if self.dependency is not None:
            doc += " dep: %s" % self.dependency
        return doc

    def get_json(self):
        json = [{'label': self.label,
                 'input_node': self.input_node.label,
                 'output_node': self.output_node.label,
                 'frequency': self.frequency,
                 'dependency': self.dependency,
                 'attributes': self.attrs}]
        return json

class DependencyGraph(Network):
    def __init__(self, label=None):
        super(DependencyGraph, self).__init__(label)


    def add_arc(self, node_a, node_b, label=None, frequency=None, dependency=None, attrs={}):
        arc = DArc(node_a, node_b, label, frequency, dependency, attrs)
        return self._add_arc(arc, node_a, node_b)
