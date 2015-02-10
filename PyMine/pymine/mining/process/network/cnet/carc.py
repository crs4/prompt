__author__ = 'paolo'

from pymine.mining.process.network.dependency.darc import DArc as DArc

class CArc(DArc):

    def __init__(self, id=None, name=None, input_node=None, output_node=None):
        super(CArc, self).__init__(id=id, name=name, input_node=input_node, output_node=output_node)
        self.input_bindings = {}
        self.output_bindings = {}

    def __str__(self):
        doc = "name="+self.name+" " \
        "input_node="+str(self.input_node)+" " \
        "output_node="+str(self.output_node)+" " \
        "frequency="+str(self.frequency)+ " " \
        "dependency="+str(self.dependency)
        return doc
