import unittest
import logging
from datetime import datetime as dt

from pymine.mining.process.eventlog import *
# logging.basicConfig(level=logging.DEBUG, format='%(filename)s:%(lineno)s %(message)s')
TIME_FORMAT = '%Y-%m-%d %H:%M:%S'


class ProcessTest(unittest.TestCase):

    def __init__(self, label):
        super(ProcessTest, self).__init__(label)

    def test_process(self):
        process = Process(_id="p1")
        activity_a = process.add_activity(Activity("A"))
        activity_b = process.add_activity(Activity("B"))

        self.assertEqual(len(process.activities), 2)

        case1 = Case("c1")
        case2 = Case("c2")
        process.add_case(case1)
        process.add_case(case2)


        # Event 1
        dt1 = dt.strptime("2014-12-25 00:00:01", TIME_FORMAT)
        event1 = Event(name=activity_a.name, timestamp=dt1, resources=["R1"])
        case1.add_event(event1)

        # Event 2
        dt2 = dt.strptime("2014-12-25 00:00:02", TIME_FORMAT)
        event2 = Event(name=activity_b.name, timestamp=dt2, resources=["R1"])
        case1.add_event(event2)

        self.assertEqual(len(case1.events), 2)
        self.assertEqual(case1.events[0].timestamp, dt1)
        self.assertEqual(case1.events[1].timestamp, dt2)
        self.assertEqual(case1.events[0].resources, ["R1"])
        self.assertEqual(case1.events[1].resources, ["R1"])

        # Event 3
        dt3 = dt.strptime("2014-12-25 00:00:02", TIME_FORMAT),
        event3 = Event(name=activity_a.name, timestamp=dt3, resources=["R2"])
        case2.add_event(event3)

        self.assertEqual(len(case2.events), 1)
        self.assertEqual(case2.events[0].timestamp, dt3)
        self.assertEqual(case2.events[0].resources, ["R2"])


if __name__ == '__main__':
    unittest.main()
