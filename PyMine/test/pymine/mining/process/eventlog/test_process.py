import unittest
import logging
from datetime import datetime as dt

from pymine.mining.process.eventlog import *
# logging.basicConfig(level=logging.DEBUG, format='%(filename)s:%(lineno)s %(message)s')
TIME_FORMAT = '%Y-%m-%d %H:%M:%S%f'


class ProcessTest(unittest.TestCase):

    def __init__(self, label):
        super(ProcessTest, self).__init__(label)

    def test_process(self):
        process = Process(_id="p1")
        activity_a = process.add_activity(name="A")
        activity_b = process.add_activity(name="B")

        self.assertEqual(len(process.activities), 2)

        case1 = Case(process, "c1")
        case2 = Case(process, "c2")

        # Event 1
        activity_a_instance = case1.add_activity_instance(activity_a)
        event1 = activity_a_instance.add_event(activity_a_instance)
        dt1 = dt.strptime("2014-12-25 00:00:01", TIME_FORMAT)
        event1.timestamp = dt1
        event1.resources.append("R1")

        # Event 2
        activity_b_instance = case1.add_activity_instance(activity_b)
        event2 = activity_b_instance.add_event(activity_b_instance)
        dt2 = dt.strptime("2014-12-25 00:00:02", TIME_FORMAT)
        event2.timestamp = dt2
        event2.resources.append("R1")

        self.assertEqual(len(case1.activity_instances), 2)
        self.assertEqual(len(case1.events), 2)
        self.assertEqual(case1.events[0].timestamp, dt1)
        self.assertEqual(case1.events[1].timestamp, dt2)
        self.assertEqual(case1.events[0].resources, ["R1"])
        self.assertEqual(case1.events[1].resources, ["R1"])

        # Event 3
        activity_a_instance = case2.add_activity_instance(activity_a)
        event3 = activity_a_instance.add_event(activity_a_instance)
        dt3 = dt.strptime("2014-12-25 00:00:02", TIME_FORMAT)
        event3.timestamp = dt3
        event3.resources.append("R2")

        self.assertEqual(len(case2.activity_instances), 1)
        self.assertEqual(len(case2.events), 1)
        self.assertEqual(case2.events[0].timestamp, dt3)
        self.assertEqual(case2.events[0].resources, ["R2"])


if __name__ == '__main__':
    unittest.main()
