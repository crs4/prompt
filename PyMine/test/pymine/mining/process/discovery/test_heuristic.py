__author__ = 'paolo'

import unittest, os

from pymine.mining.process.eventlog.factory import CsvLogFactory as CsvLogFactory
from pymine.mining.process.eventlog.log import Log as Log
import pymine.mining.process.eventlog.model.activity as activity
import pymine.mining.process.eventlog.model.activityinstance as activityinstance
import pymine.mining.process.eventlog.model.attribute as attribute
import pymine.mining.process.eventlog.model.case as case
import pymine.mining.process.eventlog.model.event as event
import pymine.mining.process.eventlog.model.process as process

from pymine.mining.process.discovery.heuristic import HeuristicMiner as HeuristicMiner
from pymine.mining.process.network.dependency.dgraph import HeuristicNetwork as HeuristicNetwork

class TestHeuristicMiner(unittest.TestCase):

    def __init__(self, label):
        super(TestHeuristicMiner, self).__init__(label)

        self.csv_test_data = ["timestamp,case_id,activity,resource,operator",
                            "2014-12-25 00:00:01,C1,A,R1,O1",
                            "2014-12-25 00:00:02,C1,B,R1,O2",
                            "2014-12-25 00:00:03,C1,C,R2,O3",
                            "2014-12-25 00:00:04,C1,B,R2,O4",
                            "2014-12-25 00:00:05,C1,D,R1,O5",
                            "2014-12-25 00:00:06,C2,A,R1,O1",
                            "2014-12-25 00:00:07,C2,B,R1,O2",
                            "2014-12-25 00:00:08,C2,D,R1,O3"]

    def create_csv_test_file(self):
        filename = "test_file.csv"
        file = open(filename, 'w')
        filepath = os.getcwd()+'/'+file.name
        for line in self.csv_test_data:
            file.write(line+'\n')
        file.close()
        return filepath

    def create_log_from_test_data(self):
        process_instances = {}
        the_process = process.Process(id="p1")

        activity_a = activity.Activity(name="A")
        activity_b = activity.Activity(name="B")
        activity_c = activity.Activity(name="C")
        activity_d = activity.Activity(name="D")

        activity_instances = {activity_a.id : activity_a,
                              activity_b.id : activity_b,
                              activity_c.id : activity_c,
                              activity_d.id : activity_d}
        activity_instance_instances = {}
        attribute_instances = {}
        event_instances = {}

        the_process.activities = activity_instances.keys()

        case1 = case.Case(id="c1", process_id=the_process.id)
        the_process.cases.append(case1.id)
        case2 = case.Case(id="c2", process_id=the_process.id)
        the_process.cases.append(case1.id)

        # Event 1
        activity_a_instance = activityinstance.ActivityInstance(id="ai1", case_id=case1.id, activity_id=activity_a.id)
        time1 = attribute.Timestamp(id="time1",attribute_value="00:00:01")
        event1 = event.Event(id="ev1", case_id=case1.id, activity_instance_id=activity_a_instance.id)
        time1.event_id = event1.id
        activity_a_instance.events.append(event1.id)
        activity_instance_instances[activity_a_instance.id] = activity_a_instance
        attribute_instances[time1.id] = time1
        event_instances[event1.id] = event1
        # Event 2
        activity_b_instance = activityinstance.ActivityInstance(id="bi1", case_id=case1.id, activity_id=activity_b.id)
        time2 = attribute.Timestamp(id="time2",attribute_value="00:00:02")
        event2 = event.Event(id="ev2", case_id=case1.id, activity_instance_id=activity_b_instance.id)
        time2.event_id = event2.id
        activity_b_instance.events.append(event2.id)
        activity_instance_instances[activity_b_instance.id] = activity_b_instance
        attribute_instances[time2.id] = time2
        event_instances[event2.id] = event2
        # Event 3
        activity_c_instance = activityinstance.ActivityInstance(id="ci1", case_id=case1.id, activity_id=activity_c.id)
        time3 = attribute.Timestamp(id="time3",attribute_value="00:00:03")
        event3 = event.Event(id="ev3", case_id=case1.id, activity_instance_id=activity_c_instance.id)
        time3.event_id = event3.id
        activity_c_instance.events.append(event3.id)
        activity_instance_instances[activity_c_instance.id] = activity_c_instance
        attribute_instances[time3.id] = time3
        event_instances[event3.id] = event3
        # Event 4
        activity_b2_instance = activityinstance.ActivityInstance(id="bi2", case_id=case1.id, activity_id=activity_b.id)
        time4 = attribute.Timestamp(id="time4",attribute_value="00:00:04")
        event4 = event.Event(id="ev4", case_id=case1.id, activity_instance_id=activity_b2_instance.id)
        time4.event_id = event4.id
        activity_b2_instance.events.append(event4.id)
        activity_instance_instances[activity_b2_instance.id] = activity_b2_instance
        attribute_instances[time4.id] = time4
        event_instances[event4.id] = event4
        # Event 5
        activity_d_instance = activityinstance.ActivityInstance(id="di1", case_id=case1.id, activity_id=activity_d.id)
        time5 = attribute.Timestamp(id="time5",attribute_value="00:00:05")
        event5 = event.Event(id="ev5", case_id=case1.id, activity_instance_id=activity_d_instance.id)
        time5.event_id = event5.id
        activity_d_instance.events.append(event5.id)
        activity_instance_instances[activity_d_instance.id] = activity_d_instance
        attribute_instances[time5.id] = time5
        event_instances[event5.id] = event5

        case1.events.append(event1.id)
        case1.events.append(event2.id)
        case1.events.append(event3.id)
        case1.events.append(event4.id)
        case1.events.append(event5.id)

        # Event 6
        activity_a2_instance = activityinstance.ActivityInstance(id="ai2", case_id=case2.id, activity_id=activity_a.id)
        time6 = attribute.Timestamp(id="time6",attribute_value="00:00:06")
        event6 = event.Event(id="ev6", case_id=case2.id, activity_instance_id=activity_a2_instance.id)
        time6.event_id = event6.id
        activity_a2_instance.events.append(event6.id)
        activity_instance_instances[activity_a2_instance.id] = activity_a2_instance
        attribute_instances[time6.id] = time6
        event_instances[event6.id] = event6
        # Event 7
        activity_b3_instance = activityinstance.ActivityInstance(id="bi3", case_id=case2.id, activity_id=activity_b.id)
        time7 = attribute.Timestamp(id="time7",attribute_value="00:00:07")
        event7 = event.Event(id="ev7", case_id=case2.id, activity_instance_id=activity_b3_instance.id)
        time7.event_id = event7.id
        activity_b3_instance.events.append(event7.id)
        activity_instance_instances[activity_b3_instance.id] = activity_b3_instance
        attribute_instances[time7.id] = time7
        event_instances[event7.id] = event7
        # Event 8
        activity_d2_instance = activityinstance.ActivityInstance(id="di2", case_id=case2.id, activity_id=activity_d.id)
        time8 = attribute.Timestamp(id="time8",attribute_value="00:00:08")
        event8 = event.Event(id="ev8", case_id=case2.id, activity_instance_id=activity_d_instance.id)
        time8.event_id = event8.id
        activity_d2_instance.events.append(event8.id)
        activity_instance_instances[activity_d2_instance.id] = activity_d2_instance
        attribute_instances[time8.id] = time8
        event_instances[event8.id] = event8

        case2.events.append(event6.id)
        case2.events.append(event7.id)
        case2.events.append(event8.id)

        case_instances = {case1.id : case1,
                          case2.id :case2}
        process_instances[the_process.id] = the_process

        log = Log()
        log.events = event_instances
        log.activities = activity_instances
        log.activity_instances = activity_instance_instances
        log.attributes = attribute_instances
        log.cases = case_instances
        log.processes = process_instances
        return log


    def test_mine(self):
        hm = HeuristicMiner()
        filepath = self.create_csv_test_file()
        log_factory = CsvLogFactory(filepath)
        file_log = log_factory.create_log()
        loginfo = log_factory.create_loginfo()
        hnet = hm.mine(file_log)
        print hnet.id
        #print hnet.nodes
        for node in hnet.nodes:
            print "Node: "+str(hnet.nodes[node])
        #print hnet.arcs
        for arc in hnet.arcs:
            print "Arc: "+str(hnet.arcs[arc])


        self.assertTrue(True)

def suite():
    suite = unittest.TestSuite()
    suite.addTest(TestHeuristicMiner('test_mine'))
    return suite

if __name__ == '__main__':
    runner = unittest.TestHeuristicMiner(verbosity=2)
    runner.run(suite())