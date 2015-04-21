from pymine.mining.process.network import Network, Arc, Node


class DArc(Arc):

    def __init__(self, start_node, end_node, label=None, frequency=0, dependency=0.0, attrs=None):
        super(DArc, self).__init__(start_node, end_node, label, frequency, attrs)
        self.dependency = dependency

    def __str__(self):
        doc = super(DArc, self).__str__()
        if self.dependency is not None:
            doc += " dep: %s" % self.dependency
        return doc

    def get_json(self):
        json = [{'label': self.label,
                 'start_node': self.start_node.label,
                 'end_node': self.end_node.label,
                 'frequency': self.frequency,
                 'dependency': self.dependency,
                 'attributes': self.attrs}]
        return json


class DependencyGraph(Network):
    def __init__(self, label=None):
        super(DependencyGraph, self).__init__(label)
        self.start_node = None
        self.end_node = None

    def add_arc(self, node_a, node_b, label=None, frequency=None, dependency=None, attrs=None):
        arc = DArc(node_a, node_b, label, frequency, dependency, attrs)
        return self._add_arc(arc, node_a, node_b)


def get_dependency_graph_from_json(json):
    try:
        origin = json[0]
        label = origin['label']
        the_net = DependencyGraph(label)
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
                                frequency=arc['frequency'], dependency=arc['dependency'], attrs=arc['attributes'])
        return the_net
    except Exception, e:
        print("An error occurred while trying to create a Network from a json")
        print(e.message)
        return None