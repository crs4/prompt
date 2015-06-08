import unittest
import os
import logging
from datetime import datetime as dt
from prompt.mining.process.eventlog.factory import CsvLogFactory, create_log_from_xes, \
    create_log_from_file, FAKE_START, FAKE_END, create_process_log_from_list
from prompt.mining.process.eventlog.log import Log
from prompt.mining.process.eventlog import *
TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
# logging.basicConfig(level=logging.DEBUG, format='%(filename)s:%(lineno)s %(message)s')
XES_PATH = os.path.join(os.path.dirname(__file__), '../../../../../dataset/reviewing_short.xes')
CSV_PATH = os.path.join(os.path.dirname(__file__), '../../../../../dataset/pg_4.csv')
AVRO_PATH = os.path.join(os.path.dirname(__file__), '../../../../../dataset/pg_4.avro')


class TestFactory(unittest.TestCase):

    def __init__(self, label):
        super(TestFactory, self).__init__(label)

        self.csv_test_data = [
            "timestamp,case_id,activity,resource,operator,lifecycle",
            "2014-12-25 00:00:01,C1,A,R1,O1,START",
            "2014-12-25 00:00:01,C1,A,R1,O1,END",
            "2014-12-25 00:00:02,C1,B,R1,O2,END",
            "2014-12-25 00:00:03,C1,C,R2,O3,END",
            "2014-12-25 00:00:04,C1,B,R2,O4,END",
            "2014-12-25 00:00:05,C1,D,R1,O5,END",
            "2014-12-25 00:00:06,C2,A,R1,O1,START",
            "2014-12-25 00:00:06,C2,A,R1,O1,END",
            "2014-12-25 00:00:07,C2,B,R1,O2,END",
            "2014-12-25 00:00:08,C2,D,R1,O3,END"
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
        event1.timestamp = dt.strptime("2014-12-25 00:00:01", TIME_FORMAT)
        event1.resources.append("R1")
        # Event 2
        activity_b_instance = case1.add_activity_instance(activity_b)
        event2 = activity_b_instance.add_event(activity_b_instance)
        event2.timestamp = dt.strptime("2014-12-25 00:00:02", TIME_FORMAT)
        event2.resources.append("R1")
        # Event 3
        activity_c_instance = case1.add_activity_instance(activity_c)
        event3 = activity_c_instance.add_event(activity_c_instance)
        event3.timestamp = dt.strptime("2014-12-25 00:00:03", TIME_FORMAT)
        event3.resources.append("R2")
        # Event 4
        activity_b_instance = case1.add_activity_instance(activity_b)
        event4 = activity_b_instance.add_event(activity_b_instance)
        event4.timestamp = dt.strptime("2014-12-25 00:00:04", TIME_FORMAT)
        event4.resources.append("R2")
        # Event 5
        activity_d_instance = case1.add_activity_instance(activity_d)
        event5 = activity_d_instance.add_event(activity_d_instance)
        event5.timestamp = dt.strptime("2014-12-25 00:00:05", TIME_FORMAT)
        event5.resources.append("R1")
        # Event 6
        activity_a_instance = case2.add_activity_instance(activity_a)
        event6 = activity_a_instance.add_event(activity_a_instance)
        event6.timestamp = dt.strptime("2014-12-25 00:00:06", TIME_FORMAT)
        event6.resources.append("R1")
        # Event 7
        activity_b_instance = case2.add_activity_instance(activity_b)
        event7 = activity_b_instance.add_event(activity_b_instance)
        event7.timestamp = dt.strptime("2014-12-25 00:00:07", TIME_FORMAT)
        event7.resources.append("R1")
        # Event 8
        activity_d_instance = case2.add_activity_instance(activity_d)
        event8 = activity_d_instance.add_event(activity_d_instance)
        event8.timestamp = dt.strptime("2014-12-25 00:00:08", TIME_FORMAT)
        event8.resources.append("R1")
        log = Log()
        log.add_case(case1)
        log.add_case(case2)
        return log

    def test_create_log(self):
        # test_log = self.create_log_from_test_data()
        file_path = self.create_csv_test_file()
        log_factory = CsvLogFactory(file_path, create_process=True)
        log = log_factory.create_log()
        #self.assertEqual(len(log.processes), 1)

        case1, case2 = log.cases
        self.assertTrue(len(case1.activity_instances), 5)
        self.assertTrue(len(case2.activity_instances), 3)
        self.assertTrue(len(case1.events), 5)
        self.assertTrue(len(case2.events), 3)

        process = log.process
        act_a = process.get_activity_by_name('A')
        act_b = process.get_activity_by_name('B')
        act_c = process.get_activity_by_name('C')
        act_d = process.get_activity_by_name('D')

        self.assertEqual(len(process.activities), 4)
        self.assertEqual(len(act_a.activity_instances), 2)
        self.assertEqual(len(act_b.activity_instances), 3)
        self.assertEqual(len(act_c.activity_instances), 1)
        self.assertEqual(len(act_d.activity_instances), 2)

        self.assertEqual(len(case1.events), 6)
        self.assertEqual(len(case2.events), 4)

        for index, e in enumerate(self.csv_test_data[1:7]):
            data = e.split(',')
            self.assertEqual(case1.events[index].timestamp, dt.strptime(data[0], TIME_FORMAT))
            self.assertEqual(case1.events[index].name, data[2])
            self.assertEqual(case1.events[index].resources, data[3])

        for index,e in enumerate(self.csv_test_data[7:]):
            data = e.split(',')
            self.assertEqual(case2.events[index].timestamp, dt.strptime(data[0], TIME_FORMAT))
            self.assertEqual(case2.events[index].name, data[2])
            self.assertEqual(case2.events[index].resources, data[3])

    def test_process_log_factory(self):

        test_case = [['A', 'B'], ['A']]

        log = create_process_log_from_list(test_case)
        self.assertEqual(len(log.cases), 2)

        self.assertEqual([e.name for e in log.cases[0].events], test_case[0])
        self.assertEqual([e.name for e in log.cases[1].events], test_case[1])

        process = log.cases[1].process
        self.assertEqual(set([a.name for a in process.activities]), {'A', 'B'})
        self.assertEqual(len(process.get_activity_by_name('A').activity_instances), 2)
        self.assertEqual(len(process.get_activity_by_name('B').activity_instances), 1)

        self.assertEqual(len(process.get_activity_by_name('A').activity_instances[0].events), 1)
        self.assertEqual(len(process.get_activity_by_name('A').activity_instances[1].events), 1)

        self.assertEqual(len(process.get_activity_by_name('B').activity_instances[0].events), 1)

    def test_create_log_from_xes(self):
        log = create_log_from_xes(XES_PATH)
        self.assertEqual(len(log.cases), 5)

    def test_create_log_from_file(self):
        log = create_log_from_file(XES_PATH, True, True)
        self.assertEqual(len(log.cases), 5)
        case0 = log.cases[0]
        self.assertEqual(case0.events[0].name, FAKE_START)
        self.assertEqual(case0.events[-1].name, FAKE_END)

    def test_create_log_from_file_2(self):
        log = create_log_from_file(CSV_PATH, True, True)
        self.assertEqual(len(log.cases), 6)
        case0 = log.cases[0]
        self.assertEqual(case0.events[0].name, FAKE_START)
        self.assertEqual(case0.events[-1].name, FAKE_END)

    def test_create_log_from_avro_(self):
        log = create_log_from_file(AVRO_PATH)
        c = 0
        for i in log.cases:
            c += 1
        self.assertEqual(c, 6)

