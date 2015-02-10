__author__ = 'paolo'

from pymine.mining.process.network.dependency.dnode import DNode as DNode

class CNode(DNode):

    def __init__(self, id=None, name=None, input_arcs=None, output_arcs=None, frequency=None):
        super(CNode, self).__init__(id=id, name=name, input_arcs=input_arcs, output_arcs=output_arcs)
        self.frequency = frequency

    def __str__(self):
        doc = "name="+self.name+" " \
        "frequency="+str(self.frequency)+" " \
        "input_arcs="+str(self.input_arcs)+" " \
        "output_arcs="+str(self.output_arcs)
        return doc
