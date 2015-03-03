from abc import ABCMeta, abstractmethod, abstractproperty
import importlib


class BaseGraph(object):

    @abstractmethod
    def add_node(self, node):
        pass

    @abstractmethod
    def add_edge(self, start_node, end_node, data=None):
        pass

    @abstractmethod
    def shortest_path(self, attribute=None):
        pass

    @abstractproperty
    def nodes(self):
        pass

    @abstractproperty
    def edges(self):
        pass

    @abstractmethod
    def draw(self):
        pass


def graph_factory(impl):
    module = importlib.import_module('pymine.mining.process.network.graph.' + impl)
    return module.Graph()