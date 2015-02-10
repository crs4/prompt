__author__ = 'paolo'

import uuid

class Network(object):

    def __init__(self, id=None, nodes=None, arcs=None):
        if id:
            self.id = id
        else:
            self.id = uuid.uuid4()

        if nodes:
            self.nodes = nodes
        else:
            self.nodes = {}

        if arcs:
            self.arcs = arcs
        else:
            self.arcs = {}