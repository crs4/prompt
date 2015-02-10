__author__ = 'paolo'

import uuid

class Node(object):

    def __init__(self, id=None, name=None, input_arcs=None, output_arcs=None):
        if id:
            self.id = id
        else:
            self.id = uuid.uuid4()

        if name:
            self.name = name
        else:
            self.name = name

        if input_arcs:
            self.input_arcs = input_arcs
        else:
            self.input_arcs = []

        if output_arcs:
            self.output_arcs = output_arcs
        else:
            self.output_arcs = []