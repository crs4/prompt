from datetime import datetime
import xml.etree.ElementTree as ET
from xml.etree import cElementTree
from prompt.mining.process.network.bpmn import BPMNDiagram, ParallelGateway, ExclusiveGateway
import xml.dom.minidom as MD
#import uuid
import  random

class BPMN2Serializer():
    def __init__(self, bpmn_network):
        self.bpmn = bpmn_network
        self.root = ET.Element('semantic:definitions',  Id=self.bpmn.label, name= self.bpmn.label)
        self.root.set('xmlns:semantic', "http://www.omg.org/spec/BPMN/20100524/MODEL")
        self.root.set('targetNamespace', "http://www.trisotech.com/definitions/_1275940932088")
        self.root.set('xmlns:xsi', "http://www.w3.org/2001/XMLSchema-instance")
        self.root.set('xmlns:di', "http://www.omg.org/spec/DD/20100524/DI")
        self.root.set('xmlns:bpmndi', 'http://www.omg.org/spec/BPMN/20100524/DI')
        self.root.set('xmlns:dc', "http://www.omg.org/spec/DD/20100524/DC")



        #self.map = {'__1275940932088' : "http://www.trisotech.com/definitions/_1275940932088",
        #                   'xsi' : "http://www.w3.org/2001/XMLSchema-instance",
        #                   'di' : "http://www.omg.org/spec/DD/20100524/DI",
        #                   'bpmndi' : "http://www.omg.org/spec/BPMN/20100524/DI",
        #                   'dc' : "http://www.omg.org/spec/DD/20100524/DC",
        #                   'semantic' : "http://www.omg.org/spec/BPMN/20100524/MODEL" }
        self.node_ids = {}
        self.arc_ids = {}
        #register namespaces
        ET._namespace_map['semantic'] = "http://www.omg.org/spec/BPMN/20100524/MODEL"
        ET._namespace_map['dc'] = "http://www.omg.org/spec/DD/20100524/DC"
        ET._namespace_map['bpmndi'] = "http://www.omg.org/spec/BPMN/20100524/DI"
        ET._namespace_map['di'] = "http://www.omg.org/spec/DD/20100524/DI"
        ET._namespace_map['xsi'] = "http://www.w3.org/2001/XMLSchema-instance"
        ET._namespace_map['__1275940932088'] = "http://www.trisotech.com/definitions/_1275940932088"


    def _get_random_coordinates(self):
        return { 'x' : str(float(random.randint(20, 1000))),
                 'y' : str(float(random.randint(20, 550)))

        }

    def _create_participants(self):
        #TODO: Support more than one participant
        coll = ET.SubElement(self.root, 'semantic:collaboration', id = 'COLL_1')
        self.collaboration_id = 'COLL_1'
        participant_id = 'Part'
        participant = ET.SubElement(coll, 'semantic:participant', id = participant_id)
        self.participant_id = participant_id

    def _create_process(self):
        process = ET.SubElement(self.root, 'semantic:process', id = self.bpmn.label)
        self._create_lanes(process)
        self._create_start_event(process)
        self._create_end_events(process)
        self._create_tasks(process)
        self._create_gateways(process)
        self._create_sequence_flows(process)
        self._create_bpmn_diagram()

    def _create_lanes(self, process):
        #TODO: Support more than one lane
        lane_set = ET.SubElement(process, 'semantic:laneSet', id='Pool')
        lane = ET.SubElement(lane_set, 'semantic:lane', id='Lane', name='Lane')
        for node in self.bpmn.nodes:
            self._create_node_activity(lane, node)

    def _create_node_activity(self, lane, node):
        node_id = node.label
        self.node_ids[node.label] = node_id
        n = ET.SubElement(lane, 'semantic:flowNodeRef')
        n.text = node_id

    def _create_start_event(self, process):
        start = self.bpmn.get_initial_nodes()[0]
        start_event = ET.SubElement(process, 'semantic:startEvent', id=self.node_ids[start.label], name=start.label )

    def _create_end_events(self, process):
        ends = self.bpmn.get_final_nodes()
        for e in ends:
            end_event = ET.SubElement(process, 'semantic:endEvent', id = self.node_ids[e.label], name=e.label)

    def _create_tasks(self, process):
        #suppose that each activity is a task ()excluding gateways and start and end events
        tasks = (set(self.bpmn.get_activities()).difference(self.bpmn.get_initial_nodes())).difference(set(self.bpmn.get_final_nodes()))
        for t in tasks:
            task = ET.SubElement(process, 'semantic:task', id = self.node_ids[t.label], name=t.label)

    def _create_gateways(self, process):
        for gw in self.bpmn.get_gateways():
            if isinstance(gw, ParallelGateway):
                type = 'semantic:parallelGateway'
            else:
                type = 'semantic:exclusiveGateway'
            if len(gw.input_arcs) < len(gw.output_arcs):
                direction = 'Diverging'
            else:
                direction = 'Converging'
            gateway = ET.SubElement(process, type, id = self.node_ids[gw.label], name=gw.label, gatewayDirection=direction)
            for input_node in gw.input_nodes:
                incoming = ET.SubElement(gateway, 'semantic:incoming')
                incoming.text = self.node_ids[input_node.label]
            for output_node in gw.output_nodes:
                outcoming = ET.SubElement(gateway, 'semantic:outgoing')
                outcoming.text = self.node_ids[output_node.label]

    def _create_sequence_flows(self, process):
        for arc in self.bpmn.arcs:
            source = self.node_ids[arc.start_node.label]
            target =  self.node_ids[arc.end_node.label]
            sf_id = source+'_'+target
            self.arc_ids[arc] = sf_id
            ET.SubElement(process, 'semantic:sequenceFlow', id=sf_id, name = "", sourceRef=source, targetRef=target)


    def _create_bpmn_diagram(self):
        diagram = ET.SubElement(self.root, 'bpmndi:BPMNDiagram', documentation="" , id="Trisotech.Visio-_6",
                                name="%s_Diagram"%self.bpmn.label, resolution="96.00000267028808")

        #create lane tag
        lane = ET.SubElement(diagram, 'bpmndi:BPMNPlane', bpmnElement=self.collaboration_id)

        #draw lane rectangle
        lane_shape = ET.SubElement(lane, 'bpmndi:BPMNShape', bpmnElement=self.participant_id, isHorizontal="true", id='Trisotech.Visio_%s'%self.participant_id)
        self.lane_shape_draw = { 'x' : '12.0',
                                 'y' : '12.0',
                                 'height' : '600.0',
                                 'width' : '1044.0'

        }

        lane_shape_coordinates = ET.SubElement(lane_shape, 'dc:Bounds', height=self.lane_shape_draw['height'], width=self.lane_shape_draw['width'], \
                                               x=self.lane_shape_draw['x'], y=self.lane_shape_draw['y'])

        #create node shapes tags
        self.node_coordinates = {}
        for n in self.bpmn.nodes:
            self.node_coordinates[n.label] = self._get_random_coordinates()
            node_shape = ET.SubElement(lane, 'bpmndi:BPMNShape', bpmnElement=self.node_ids[n.label], id="Trisotech.Visio_%s"%self.node_ids[n.label])
            #draw activities
            ET.SubElement(node_shape,'dc:Bounds', height='30.0', width='30.0', x=self.node_coordinates[n.label]['x'], y=self.node_coordinates[n.label]['y'])


        for a in self.bpmn.arcs:
            edge = ET.SubElement(lane, 'bpmndi:BPMNEdge',bpmnElement=self.arc_ids[a], id="Trisotech.Visio_%s"%self.arc_ids[a])
            #Draw the arc defining start and end point of the arc: start point is according the coordinates of the start node, the
            #same for the end node
            arc_start_coordinates = self.node_coordinates[a.start_node.label]
            arc_end_coordinates = self.node_coordinates[a.end_node.label]
            ET.SubElement(edge, 'di:waypoint', x=str(float(arc_start_coordinates['x'])+30/2), y=str(float(arc_start_coordinates['y'])+30/2 ))
            ET.SubElement(edge, 'di:waypoint', x=str(float(arc_end_coordinates['x'])+30/2), y=str(float(arc_end_coordinates['y'])+30/2 ))
            #ET.SubElement(edge, 'di:waypoint', x = arc_start_coordinates['x'], y=arc_start_coordinates['y'])
            #ET.SubElement(edge, 'di:waypoint', x = arc_end_coordinates['x'], y=arc_end_coordinates['y'])

    def serialize(self):
        self._create_participants()
        self._create_process()


    def generate_bpmn_file(self, file_dir):
        """
        Creates the XPDL file
        :param file_dir:The file name ([directory]/[*.xpdl])
        """
        f = open(file_dir, 'w')
        rough_string = ET.tostring(self.root, 'utf-8')
        #print rough_string
        reparsed = MD.parseString(rough_string)
        #cElementTree.ElementTree(self.root).write(f)
        f.write(reparsed.toprettyxml(indent="\t"))
        f.close()



