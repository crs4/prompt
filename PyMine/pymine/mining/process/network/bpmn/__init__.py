from pymine.mining.process.network import Network
from pymine.mining.process.network import Node
from pymine.mining.process.network import Arc

class BPMNDiagram(Network):
    """
    BPMN network class
    """

    def __init__(self, label, process_name=None, process_desc=None,  process_participants=None):
        super(BPMNDiagram, self).__init__(label)
        self.label = label
        self.process_name = process_name
        self.process_desc = process_desc
        self.process_participants = process_participants
        self.attributes = {}

    def _create_nodes(self, *labels):
        """
        Adds a list of activities to the network
        :param labels: The array containing the labels of the activities to add
        :return:
        """
        return [Activity(label, self) for label in labels]

    def add_node(self, label, frequency = None, attrs= {}, node_type='activity'):
        """
        Adds a single node to the network, of the given type. A node can be an activity, or a gateway (Parallel, Exclusive,
        Inclusive). Different gateway types are instances of the related class.
        :param label: The label to assign to the new node
        :param frequency: The frequency to assign to the new node
        :param attrs: A list of attributes of the new node
        :param node_type: The type of the node: an activity, or one of the available gateways
        :return:
        """
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

    def remove_activity(self, act):
        """
        Remove an activity from the network. All incoming and outcoming arcs are removed accordingly
        :param act: The activity to remove
        :return:
        """
        arcs_to_remove = []
        #remove arcs related to the node
        for output_arc in act.output_arcs:
                arcs_to_remove.append(output_arc)
        for input_arc in act.input_arcs:
                arcs_to_remove.append(input_arc)
        for arc in arcs_to_remove:
            self.remove_arc(arc)
        self._nodes.remove(act)

    def get_gateways(self):
        """
        Returns the list of all gateway nodes in the BPMN network
        :return: A list containing all gateway nodes of the network
        """
        gateways = list()
        for node in self.nodes:
            if isinstance(node, ParallelGateway) or isinstance(node, ExclusiveGateway) or isinstance(node, InclusiveGateway):
                gateways.append(node)
        return gateways


    def get_activities(self):
        """
        Returns the list of all activityy nodes in the BPMN network
        :return: A list containing all activity nodes of the network
        """
        activities = list()
        for node in self.nodes:
            if  not isinstance(node, ParallelGateway) and not  isinstance(node, ExclusiveGateway) and not isinstance(node, InclusiveGateway):
                activities.append(node)
        return activities

    def is_gateway(self, node):
        """
        Returns True if the target node is a gateway node of the BPMN network
        :param node: The target node of the network
        :return: True if the node is a gateway node, else False
        """
        if isinstance(node, ParallelGateway) or isinstance(node, ExclusiveGateway) or isinstance(node, InclusiveGateway):
            return True
        return False


class Activity(Node):
    """
    Models an Activity
    """
    def __init__(self, label, net, frequency=None, description=None,  event=None):
        super(Activity, self).__init__(label, net, frequency)
        self.label = label
        self.description = description
        self.event = event
        self.attributes = {}
        self.is_start = False
        self.is_end = False


class Transaction(Arc):
    """
    Models a transaction
    """

    def __init__(self, label, start_node, end_node, frequency=None, desc=None ):
        super(Transaction, self).__init__(label, start_node, end_node, frequency)
        self.label = label
        self.desc = desc
        self.attributes = {}
        self.condition = None


class ParallelGateway(Node):
    """
    Models a Parallel Gateway
    """
    def __init__(self, label, net, frequency=None, desc=None):
        super(ParallelGateway, self).__init__(label, net, frequency)
        self.label = label
        self.desc = desc
        self.attributes = {}


class InclusiveGateway(Node):
    """
    Models an Inclusive Gateway
    """
    def __init__(self, label, net, frequency=None, desc=None):
        super(InclusiveGateway, self).__init__(label, net, frequency)
        self.label = label
        self.desc = desc
        self.attributes = {}


class ExclusiveGateway(Node):
    """
    Models an Exclusive gatewat
    """
    def __init__(self, label, net, frequency=None, desc=None):
        super(ExclusiveGateway, self).__init__(label, net, frequency)
        self.label = label
        self.desc = desc
        self.attributes = {}

