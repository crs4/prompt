__author__ = 'paolo'

from pymine.mining.process.network.network import Network as Network

class DependencyGraph(Network):

    def __init__(self):
        super(DependencyGraph, self).__init__()