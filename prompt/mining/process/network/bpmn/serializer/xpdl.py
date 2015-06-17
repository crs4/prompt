from datetime import datetime
from xml.etree.ElementTree import Element, SubElement
from xml.etree import cElementTree
from prompt.mining.process.network.bpmn import BPMNDiagram, ParallelGateway, ExclusiveGateway

class XPDLSerializer():
    def __init__(self, bpmn_network):
        self.bpmn = bpmn_network
        #self.nsmap = {'xsi' : "http://www.wfmc.org/2008/XPDL2.1"}
        self.root = Element('Package',  Id=self.bpmn.label, name= self.bpmn.label)

    def _create_header(self):
        #root element
        package_header = SubElement(self.root, 'PackageHeader')
        version = SubElement(package_header, 'XPDLVersion')
        version.text = '2.1'
        created = SubElement(package_header, 'Created')
        created.text = datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')

    def _create_participants(self):
        #TODO: Support more than one participant
        participants = SubElement(self.root, 'Participants')
        participant = SubElement(participants, 'Participant',  Id='Process Participant')

    def _create_pools(self):
        #TODO: Multiple lanes support
        pools = SubElement(self.root, 'Pools')
        pool = SubElement(pools, 'Pool', id = 'pool', BoundaryVisible='True', Process=self.bpmn.label)
        lanes = SubElement(pool, 'Lanes')
        lane = SubElement(lanes, 'Lane', Id='lane', Name='lane')

    def _create_activity(self, activities, activity):
        act = SubElement(activities, 'Activity', Id = activity.label)
        if activity.is_first():
            self._create_start_end_events(act, 'START')
        elif activity.is_last():
            self._create_start_end_events(act, 'END')

    def _create_start_end_events(self, activity, type):
        event = SubElement(activity, 'Event')
        if type == 'START':
            SubElement(event, 'StartEvent')
        elif type == 'END':
            SubElement(event, 'EndEvent')


    def _create_gateway(self, activities, activity) :
        act = SubElement(activities, 'Activity', Id = activity.label)
        gateway_type = 'Exclusive'
        if isinstance(activity, ParallelGateway):
            gateway_type = 'Parallel'
        SubElement(act, 'Route', GatewayType=gateway_type)


    def _create_activities(self, wf_process):
        activities = SubElement(wf_process, 'Activities')
        for activity in self.bpmn.nodes:
            if not isinstance(activity, ParallelGateway) and not isinstance(activity, ExclusiveGateway):
                self._create_activity(activities, activity)
            else:
                self._create_gateway(activities, activity)


    def _create_transitions(self, wf_process):
        transition_counter = 0
        transitions = SubElement(wf_process, 'Transitions')
        for transition in self.bpmn.arcs:
            transition_id = '%s_tra_%s' %(self.bpmn.label, transition_counter)
            self._create_transition(transitions, transition,transition_id)
            transition_counter +=1

    def _create_transition(self, transitions, transition, transition_id):
        SubElement(transitions, 'Transition', Id=transition_id,From=transition.start_node.label, To=transition.end_node.label)




    def _create_workflow_process(self):
        wf_processes = SubElement(self.root, 'WorkflowProcesses')
        wf_process = SubElement(wf_processes, 'WorkflowProcess', AccessLevel='PUBLIC', Id=self.bpmn.label)
        wf_process_header = SubElement(wf_process, 'ProcessHeader')
        wf_process_created = SubElement(wf_process_header, 'Created')
        wf_process_created.text = datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')
        return wf_process

    def serialize(self):
        """
        Serializes a BPMN pbject into XPDL, according to the structure of the object
        :return:
        """
        #root element
        self._create_header()
        self._create_participants()
        self._create_pools()
        wf_process = self._create_workflow_process()
        self._create_activities(wf_process)
        self._create_transitions(wf_process)

    def generate_xpdl_file(self, file_dir):
        """
        Creates the XPDL file
        :param file_dir:The file name ([directory]/[*.xpdl])
        """
        f = open(file_dir, 'w')
        cElementTree.ElementTree(self.root).write(f)
        f.close()