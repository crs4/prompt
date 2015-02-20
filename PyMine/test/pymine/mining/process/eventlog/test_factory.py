import unittest
import os
from pymine.mining.process.eventlog.factory import CsvLogFactory as CsvLogFactory
from pymine.mining.process.eventlog.log import Log as Log
from pymine.mining.process.eventlog import *

class TestFactory(unittest.TestCase):

    def __init__(self, label):
        super(TestFactory, self).__init__(label)

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
        the_process = Process(_id="p1")
        case1 = the_process.add_case("c1")
        case2 = the_process.add_case("c2")
        activity_a = the_process.add_activity(activity_name="A")
        activity_b = the_process.add_activity(activity_name="B")
        activity_c = the_process.add_activity(activity_name="C")
        activity_d = the_process.add_activity(activity_name="D")
        # Event 1
        activity_a_instance = case1.add_activity_instance(activity_a)
        event1 = case1.add_event(activity_a_instance)
        event1.timestamp = "00:00:01"
        event1.resources.append("R1")
        # Event 2
        activity_b_instance = case1.add_activity_instance(activity_b)
        event2 = case1.add_event(activity_b_instance)
        event2.timestamp = "00:00:02"
        event2.resources.append("R1")
        # Event 3
        activity_c_instance = case1.add_activity_instance(activity_c)
        event3 = case1.add_event(activity_c_instance)
        event3.timestamp = "00:00:03"
        event3.resources.append("R2")
        # Event 4
        activity_b_instance = case1.add_activity_instance(activity_b)
        event4 = case1.add_event(activity_b_instance)
        event4.timestamp = "00:00:04"
        event4.resources.append("R2")
        # Event 5
        activity_d_instance = case1.add_activity_instance(activity_d)
        event5 = case1.add_event(activity_d_instance)
        event5.timestamp = "00:00:05"
        event5.resources.append("R1")
        # Event 6
        activity_a_instance = case2.add_activity_instance(activity_a)
        event6 = case1.add_event(activity_a_instance)
        event6.timestamp = "00:00:06"
        event6.resources.append("R1")
        # Event 7
        activity_b_instance = case2.add_activity_instance(activity_b)
        event7 = case1.add_event(activity_b_instance)
        event7.timestamp = "00:00:07"
        event7.resources.append("R1")
        # Event 8
        activity_d_instance = case2.add_activity_instance(activity_d)
        event8 = case1.add_event(activity_d_instance)
        event8.timestamp = "00:00:08"
        event8.resources.append("R1")
        log = Log()
        log.processes.append(the_process)
        return log

    def test_create_log(self):
        test_log = self.create_log_from_test_data()
        file_path = self.create_csv_test_file()
        log_factory = CsvLogFactory(file_path)
        file_log = log_factory.create_log()
        self.assertEqual(test_log, file_log)

    '''
    def test_create_loginfo(self):
        test_log = self.create_log_from_test_data()
        file_path = self.create_csv_test_file()
        log_factory = CsvLogFactory(file_path)
        file_log = log_factory.create_log()
        log_info = log_factory.create_loginfo()
        self.assertEqual(test_log,file_log)
    '''

def suite():
    suite = unittest.TestSuite()
    suite.addTest(TestFactory('test_create_log'))
    #suite.addTest(TestFactory('test_create_loginfo'))
    return suite

if __name__ == '__main__':
    runner = unittest.TestFactory(verbosity=2)
    runner.run(suite())