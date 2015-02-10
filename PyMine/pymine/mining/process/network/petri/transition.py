__author__ = 'paolo'

from pymine.mining.process.network.node import Node as Node

class Transition(Node):

    def __init__(self, id=None, name=None, input_arcs=None, output_arcs=None):
        super(Transition, self).__init__(id=id, name=name, input_arcs=input_arcs, output_arcs=output_arcs)

    def __str__(self):
        doc = "name="+self.name+" " \
        "input_arcs="+str(self.input_arcs)+" " \
        "output_arcs="+str(self.output_arcs)
        return doc