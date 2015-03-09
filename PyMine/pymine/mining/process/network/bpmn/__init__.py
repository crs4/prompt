from pymine.mining.process.network import Network
from pymine.mining.process.network import Node
from pymine.mining.process.network import Arc

class BPMNDiagram(Network):

    def __init__(self, label, process_name=None, process_desc=None,  process_participants=None):
        super(BPMNDiagram, self).__init__(label)
        self.label = label
        self.process_name = process_name
        self.process_desc = process_desc
        self.process_participants = process_participants
        self.attributes = {}

    def _create_nodes(self, *labels):
        return [Activity(label, self) for label in labels]

    def add_node(self, label, frequency = None, attrs= {}, node_type='activity'):
        if node_type == 'activity':
            return super(BPMNDiagram, self).add_node(label)
        elif node_type == 'inclusive_gateway':
            node = InclusiveGateway(label,frequency, attrs)
            self._nodes.append(node)
            return node
        elif node_type == 'exclusive_gateway':
            node = ExclusiveGateway(label,frequency, attrs)
            self._nodes.append(node)
            return node
        elif node_type == 'parallel_gateway':
            node = ParallelGateway(label,frequency, attrs)
            self._nodes.append(node)
            return node


class Activity(Node):
    def __init__(self, label, net, frequency=None, description=None,  event=None):
        super(Activity, self).__init__(label, net, frequency)
        self.label = label
        self.description = description
        self.event = event
        self.attributes = {}
        self.is_start = False
        self.is_end = False


class Transaction(Arc):
    def __init__(self, label, start_node, end_node, frequency=None, desc=None ):
        super(Transaction, self).__init__(label, start_node, end_node, frequency)
        self.label = label
        self.desc = desc
        self.attributes = {}
        self.condition = None


class ParallelGateway(Node):
    def __init__(self, label, net, frequency=None, desc=None):
        super(ParallelGateway, self).__init__(label, net, frequency)
        self.label = label
        self.desc = desc
        self.attributes = {}


class InclusiveGateway(Node):
    def __init__(self, label, net, frequency=None, desc=None):
        super(InclusiveGateway, self).__init__(label, net, frequency)
        self.label = label
        self.desc = desc
        self.attributes = {}


class ExclusiveGateway(Node):
    def __init__(self, label, net, frequency=None, desc=None):
        super(ExclusiveGateway, self).__init__(label, net, frequency)
        self.label = label
        self.desc = desc
        self.attributes = {}

