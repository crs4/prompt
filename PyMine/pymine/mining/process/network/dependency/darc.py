__author__ = 'paolo'

from pymine.mining.process.network.arc import Arc as Arc

class DArc(Arc):

    def __init__(self, id=None, name=None, input_node=None, output_node=None):
        super(DArc, self).__init__(id=id, name=name, input_node=input_node, output_node=output_node)
        self.frequency = 0
        self.dependency = 0.0

    def __str__(self):
        doc = "name="+self.name+" " \
        "input_node="+str(self.input_node)+" " \
        "output_node="+str(self.output_node)+" " \
        "frequency="+str(self.frequency)+ " " \
        "dependency="+str(self.dependency)
        return doc