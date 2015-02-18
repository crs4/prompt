from pymine.mining.process.network import Node, Network


class Binding(object):
    def __init__(self, node_ref, node_set, frequency=None):
        self.node_ref = node_ref
        self.node_set = node_set
        self.frequency = frequency
        self.net = self.node_ref.net


class CNode(Node):
    def __init__(self, label, net, frequency=None):
        super(CNode, self).__init__(label, net, frequency)
        self.input_bindings = set()
        self.output_bindings = set()


    def add_input_bindings(self, node_set):
        pass

    def add_output_bindings(self, node_set):
        pass



    # def __init__(self, id=None, name=None, input_arcs=None, output_arcs=None, frequency=None):
    #     super(CNode, self).__init__(id=id, name=name, input_arcs=input_arcs, output_arcs=output_arcs)
    #     self.frequency = frequency
    #     self.input_bindings = []
    #     self.output_bindings = []
    #
    # def __str__(self):
    #     doc = "name="+self.name+" " \
    #     "frequency="+str(self.frequency)+" " \
    #     "input_arcs="+str(self.input_arcs)+" " \
    #     "output_arcs="+str(self.output_arcs)
    #     return doc


class CNet(Network):

    def get_bindings(self):
        pass

    def replay_sequence(self, sequence):
        pass

    def replay_sequence_set(self, sequence_set):
        pass
