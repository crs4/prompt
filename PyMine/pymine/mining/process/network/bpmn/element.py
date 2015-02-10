__author__ = 'paolo'

from pymine.mining.process.network.node import Node as Node

class Element(Node):

    def __init__(self, id=None, name=None, input_connections=None, output_connections=None):
        super(Element, self).__init__(id=id, name=name, input_arcs=input_connections, output_arcs=output_connections)

    def __str__(self):
        doc = "name="+self.name+" " \
        "input_connections="+str(self.input_arcs)+" " \
        "output_connections="+str(self.output_arcs)
        return doc


class Activity(Element):

    def __init__(self, id=None, name=None, input_connections=None, output_connections=None):
        super(Activity, self).__init__(id=id, name=name, input_arcs=input_connections, output_arcs=output_connections)


class Gateway(Element):

    def __init__(self, id=None, name=None, type=type, input_connections=None, output_connections=None):
        super(Gateway, self).__init__(id=id, name=name, input_arcs=input_connections, output_arcs=output_connections)
        self.type = type