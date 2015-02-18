from pymine.mining.process.network import Network, Arc


class DArc(Arc):

    def __init__(self, label, input_node, output_node, frequency=None, dependency=None):
        super(Arc, self).__init__(self, label, input_node, output_node, frequency=None)
        self.dependency = None

    def __str__(self):
        doc = "name="+self.name+" " \
            "input_node="+str(self.input_node)+" " \
            "output_node="+str(self.output_node)+" " \
            "frequency="+str(self.frequency)+ " " \
            "dependency="+str(self.dependency)
        return doc


class DependencyGraph(Network):
    def __init__(self):
        super(DependencyGraph, self).__init__()

    def _create_arc(self, node_a, node_b, label):
        return DArc(label, node_a, node_b)