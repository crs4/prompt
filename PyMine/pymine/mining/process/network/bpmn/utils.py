from pymine.mining.process.network.bpmn import BPMNDiagram, ParallelGateway, ExclusiveGateway
from pymine.mining.process.network import Node
import logging

class BPMNUtils(object):
    """
    Utility class of the BPMN network. This class provides methods to analyze the BPMN object and reduce the number
    of gateways and activities by:
        1) Removing not needed gateways from the network
        2) Joining a pair of gateway and activity into one new activity
    """

    def __init__(self):
        pass

    def remove_silent_activities(self, conversion_map, bpmn):
       pass

    def reduce_gateways(self, bpmn):
        """
        Analyses the BPMN network and reduces the number of gateways. If a gateway has a single input arc and a single
        output arc and its output node is another gateway of the same type, the gateway can be deleted and a direct arc
        is created to connect the gateway input node to the other output gateway of the same type
        :param bpmn: The BPMN network object
        :return:
        """
        diagram_changed = True
        while diagram_changed:
            diagram_changed = False
            gateway_to_remove = None
            logging.debug('Checking gateways......')
            for gateway in bpmn.get_gateways():
                logging.debug('Reduce_gateways:current:'+gateway.label)
                for arc in gateway.output_arcs:
                    if bpmn.is_gateway(arc.end_node):
                        #the gateway is followed by another gateway
                        following_gateway = arc.end_node
                        logging.debug('Reduce_gateways:following:'+following_gateway.label)
                        if len(gateway.output_nodes) == 1 or len(following_gateway.input_nodes) == 1:
                            if gateway.__class__ == following_gateway.__class__: #same gw type
                                following_nodes = []
                                for out_arc in following_gateway.output_arcs:
                                    following_node = out_arc.end_node
                                    following_nodes.append(following_node)
                                prec_nodes = []
                                for in_arc in following_gateway.input_arcs:
                                    prec_node = in_arc.start_node
                                    if not gateway == prec_node:
                                        prec_nodes.append(prec_node)
                                for fnode in following_nodes:
                                    logging.debug('Reduce_gateways:adding:'+gateway.label + '->'+fnode.label)
                                    bpmn.add_arc(gateway, fnode)
                                for pnode in prec_nodes:
                                    logging.debug('Reduce_gateways:adding:'+pnode.label + '->'+gateway.label)
                                    bpmn.add_arc(pnode, gateway)
                                gateway_to_remove = following_gateway
                                diagram_changed = True
                                break
                if diagram_changed:
                    break
            if diagram_changed:
                logging.debug('Reduce_gateways:removing:'+gateway_to_remove.label)
                for a in gateway_to_remove.output_arcs:
                    logging.debug(' Out Arc to remove:'+a.start_node.label+'->'+a.end_node.label)
                for a in gateway_to_remove.input_arcs:
                    logging.debug('In Arc to remove:'+a.start_node.label+'->'+a.end_node.label)
                bpmn.remove_activity(gateway_to_remove)

    def merge_activities_and_gateways(self, bpmn):
        """
        Analyses the BPMN network in order to find a pair of activity-gateway which can be merged. This occurs when
        an activity has an output arc connecting it to a parallel gateway, and that connection is the only input arc
        of the gateway. In that case, the gateway is removed, and the activity is directly connected to each output node
        of the removed gateway.
        The same rule is applied in case of a gateway is a source node of the activity (and not an end node), removing the
        gateway and directly connecting the source nodes of the removed gateway to the activity.
        :param bpmn: The BPMN network object
        :return:
        """
        for activity in bpmn.get_activities():
            logging.debug('merge_activities_and_gateways:Current activity: '+activity.label)
            for arc in activity.output_arcs:
                if bpmn.is_gateway(arc.end_node):
                    following_gateway = arc.end_node
                    if isinstance(following_gateway, ParallelGateway):
                        if len(following_gateway.input_arcs) == 1:
                            following_nodes = []
                            for out_arc in following_gateway.output_arcs:
                                following_node = out_arc.end_node
                                following_nodes.append(following_node)
                            logging.debug('merge_activities_and_gateways:'+following_gateway.label)
                            bpmn.remove_activity(following_gateway)
                            for fnode in following_nodes:
                                bpmn.add_arc(activity, fnode)

            for arc in activity.input_arcs:
                if bpmn.is_gateway(arc.start_node):
                    preceding_gateway = arc.start_node
                    logging.debug('merge_activities_and_gateways:preceding gateway is :'+preceding_gateway.label)
                    if isinstance(preceding_gateway, ExclusiveGateway):
                        if len(preceding_gateway.output_arcs) == 1:
                            preceding_nodes = []
                            for in_arc in preceding_gateway.input_arcs:
                                preceding_node = in_arc.start_node
                                preceding_nodes.append(preceding_node)
                            bpmn.remove_activity(preceding_gateway)
                            for pnode in preceding_nodes:
                                bpmn.add_arc(pnode, activity)

    def simplify_bpmn_diagram(self, bpmn):
        if not bpmn:
            raise Exception('BPMN diagram is null')
        logging.info('Reducing gateways........')
        self.reduce_gateways(bpmn)
        logging.info('Merging activities and gateways.......')
        self.merge_activities_and_gateways(bpmn)



