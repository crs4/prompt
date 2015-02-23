import unittest
import os
import logging
from datetime import datetime as dt
from pymine.mining.process.eventlog.factory import CsvLogFactory
from pymine.mining.process.eventlog.log import Log
from pymine.mining.process.eventlog import *
logging.basicConfig(level=logging.DEBUG, format='%(filename)s:%(lineno)s %(message)s')


class TestFactory(unittest.TestCase):

    def __init__(self, label):
        super(TestFactory, self).__init__(label)

        self.csv_test_data = [
            "timestamp,case_id,activity,resource,operator",
            "2014-12-25 00:00:01,C1,A,R1,O1",
            "2014-12-25 00:00:02,C1,B,R1,O2",
            "2014-12-25 00:00:03,C1,C,R2,O3",
            "2014-12-25 00:00:04,C1,B,R2,O4",
            "2014-12-25 00:00:05,C1,D,R1,O5",
            "2014-12-25 00:00:06,C2,A,R1,O1",
            "2014-12-25 00:00:07,C2,B,R1,O2",
            "2014-12-25 00:00:08,C2,D,R1,O3"
        ]

    def create_csv_test_file(self):
        filename = "test_file.csv"
        file = open(filename, 'w')
        filepath = os.getcwd()+'/'+file.name
        for line in self.csv_test_data:
            file.write(line+'\n')
        file.close()
        return filepath

    def create_log_from_test_data(self):
        process = Process(_id="p1")
        case1 = Case(process, "c1")
        case2 = Case(process, "c2")
        activity_a = process.add_activity(name="A")
        activity_b = process.add_activity(name="B")
        activity_c = process.add_activity(name="C")
        activity_d = process.add_activity(name="D")
        # Event 1
        activity_a_instance = case1.add_activity_instance(activity_a)
        event1 = activity_a_instance.add_event(activity_a_instance)
        event1.timestamp = dt.strptime("2014-12-25 00:00:01", CsvLogFactory.TIME_FORMAT)
        event1.resources.append("R1")
        # Event 2
        activity_b_instance = case1.add_activity_instance(activity_b)
        event2 = activity_b_instance.add_event(activity_b_instance)
        event2.timestamp = dt.strptime("2014-12-25 00:00:02", CsvLogFactory.TIME_FORMAT)
        event2.resources.append("R1")
        # Event 3
        activity_c_instance = case1.add_activity_instance(activity_c)
        event3 = activity_c_instance.add_event(activity_c_instance)
        event3.timestamp = dt.strptime("2014-12-25 00:00:03", CsvLogFactory.TIME_FORMAT)
        event3.resources.append("R2")
        # Event 4
        activity_b_instance = case1.add_activity_instance(activity_b)
        event4 = activity_b_instance.add_event(activity_b_instance)
        event4.timestamp = dt.strptime("2014-12-25 00:00:04", CsvLogFactory.TIME_FORMAT)
        event4.resources.append("R2")
        # Event 5
        activity_d_instance = case1.add_activity_instance(activity_d)
        event5 = activity_d_instance.add_event(activity_d_instance)
        event5.timestamp = dt.strptime("2014-12-25 00:00:05", CsvLogFactory.TIME_FORMAT)
        event5.resources.append("R1")
        # Event 6
        activity_a_instance = case2.add_activity_instance(activity_a)
        event6 = activity_a_instance.add_event(activity_a_instance)
        event6.timestamp = dt.strptime("2014-12-25 00:00:06", CsvLogFactory.TIME_FORMAT)
        event6.resources.append("R1")
        # Event 7
        activity_b_instance = case2.add_activity_instance(activity_b)
        event7 = activity_b_instance.add_event(activity_b_instance)
        event7.timestamp = dt.strptime("2014-12-25 00:00:07", CsvLogFactory.TIME_FORMAT)
        event7.resources.append("R1")
        # Event 8
        activity_d_instance = case2.add_activity_instance(activity_d)
        event8 = activity_d_instance.add_event(activity_d_instance)
        event8.timestamp = dt.strptime("2014-12-25 00:00:08", CsvLogFactory.TIME_FORMAT)
        event8.resources.append("R1")
        log = Log()
        log.processes.append(process)
        return log

    def test_create_log(self):
        # test_log = self.create_log_from_test_data()
        file_path = self.create_csv_test_file()
        log_factory = CsvLogFactory(file_path)
        log = log_factory.create_log()
        self.assertEqual(len(log.processes), 1)

        case1, case2 = log.cases
        self.assertTrue(len(case1.activity_instances), 5)
        self.assertTrue(len(case2.activity_instances), 3)
        self.assertTrue(len(case1.events), 5)
        self.assertTrue(len(case2.events), 3)

        process = log.processes[0]
        act_a = process.get_activity_by_name('A')
        act_b = process.get_activity_by_name('B')
        act_c = process.get_activity_by_name('C')
        act_d = process.get_activity_by_name('D')

        self.assertEqual(len(process.activities), 4)
        self.assertEqual(len(act_a.activity_instances), 2)
        self.assertEqual(len(act_b.activity_instances), 3)
        self.assertEqual(len(act_c.activity_instances), 1)
        self.assertEqual(len(act_d.activity_instances), 2)

        self.assertEqual(len(case1.events), 5)
        self.assertEqual(len(case2.events), 3)

        for index,e in enumerate(self.csv_test_data[1:6]):
            data = e.split(',')
            self.assertEqual(case1.events[index].timestamp, dt.strptime(data[0], CsvLogFactory.TIME_FORMAT))
            self.assertEqual(case1.events[index].activity_name, data[2])
            self.assertEqual(case1.events[index].activity_name, data[2])
            self.assertEqual(case1.events[index].resources, [data[3]])
            self.assertEqual(len(case1.events[index].attributes), 1)
            self.assertEqual(case1.events[index].attributes[0].name, 'operator')

        for index,e in enumerate(self.csv_test_data[6:]):
            data = e.split(',')
            self.assertEqual(case2.events[index].timestamp, dt.strptime(data[0], CsvLogFactory.TIME_FORMAT))
            self.assertEqual(case2.events[index].activity_name, data[2])
            self.assertEqual(case2.events[index].activity_name, data[2])
            self.assertEqual(case2.events[index].resources, [data[3]])
            self.assertEqual(len(case2.events[index].attributes), 1)
            self.assertEqual(case2.events[index].attributes[0].name, 'operator')

        # for event in case1.events:
        #     self.assertEqual(event.timestamp, dt.strptime())





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