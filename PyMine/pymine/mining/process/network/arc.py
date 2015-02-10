__author__ = 'paolo'

import uuid

class Arc(object):

    def __init__(self, id=None, name=None, input_node=None, output_node=None):
        if id:
            self.id = id
        else:
            self.id = uuid.uuid4()

        if name:
            self.name = name
        else:
            self.name = None

        if input_node:
            self.input_node = input_node
        else:
            self.input_node = None

        if output_node:
            self.output_node = output_node
        else:
            self.output_node = None