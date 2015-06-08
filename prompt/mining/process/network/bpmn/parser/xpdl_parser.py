from prompt.mining.process.network.bpmn import BPMNDiagram, Activity, Transaction, ParallelGateway, ExclusiveGateway
from prompt.mining.process.network.bpmn.parser import xpdl
from prompt.mining.process.network.bpmn.parser.process import TextCondition

class XPDLParser():

    def __init__(self, xpdl_file=None):
        self.file = xpdl_file


    def _get_main_process(self, package):
        """
        A xpdl package obtained from the file parsing can be the representation of a single process or of a multi-process definition, where the activity elements are
        processes themselves. This method retrieves the main process from the list of all processes of the package
        """
        processes = package.keys()
        for process_id in processes:
            process_definition = package[process_id]
            #scan all the activities. If one of the activity has the same id of one of the other processes,
            # it means that the current process is the main one.
            for activity_id in process_definition.activities.keys():
                if process_definition.activities[activity_id].subflows and str(
                        process_definition.activities[activity_id].subflows[0][0]) in processes:
                    return process_id
        return False

    def _create_BPMNDiagram(self, process):
        """
        Takes the xpdl parsed definition of a process and returns the corrispondent BPMNDiagram
        :param process:
        :return:
            The BPMNDiagram Process representation
        """
        try:
            bpmn_diagram = BPMNDiagram(label = process.id)

            #bpmn_diagram.id = process.id

            for activity_id, activity in process.activities.iteritems():
                type = 'activity'
                #check if the activity is a routing one
                for transition in activity.outgoing:
                    if isinstance(transition.condition, TextCondition):
                        type = 'exclusive_gateway'
                act = bpmn_diagram.add_node(activity_id, node_type=type)
                act_def_attributes = activity.attributes  #iterate activity attributes
                for item in act_def_attributes.items():
                    if item[0] == 'event' and type == 'activity':  #the attribute is of event type: so an event object has to be created
                        act.event = item[1]
                #check if the activity is a start or a end one
                # if len(activity.incoming) == 0:
                #     act.is_start == True
                # if len(activity.outgoing) == 0:
                #     act.is_end == True

                #check activity type: Task(Activity) or Routing
                # for o in activity.outgoing:
                #     if isinstance(o.condition, TextCondition):
                #         act.type = 'route'
                #     else:
                #         act.type = 'activity'

                #bpmn_diagram.add_activity(act)

            for transition in process.transitions:
                from_act = bpmn_diagram.get_node_by_label(transition.from_)
                to_act = bpmn_diagram.get_node_by_label(transition.to)
                bpmn_diagram.add_arc(from_act, to_act, transition.id)

            return bpmn_diagram

        except Exception, e :
            print "Error occurred during Diagram creation: %s"%e


    def parse(self):
        """
        Parses a XPDL process files into one or more BPMNDiagram network objects
        Args:
            file_path (string): The path of the XPDL process file

        Returns:
            * A list of one or more BPMNDiagram objects

        Raises:
            Exception if the file is not a valid XPDL file
    """
        try:
            xpdl_file = open(self.file, 'r')
            process_package = xpdl.read(xpdl_file)
            main_process_id = self._get_main_process(process_package)

            #if the package contains a single process, return the BPMNDiagram belonging to that process
            if not main_process_id:
                return self._create_BPMNDiagram(process_package[process_package.keys()[0]])
            else:
                #To be implemented
                raise NotImplementedError

        except Exception, e:
            print 'Exception occurred: %s'%e.message




