import unittest
import logging
from datetime import datetime as dt
from prompt.mining.process.eventlog.log import Log, Log
from prompt.mining.process.eventlog.exceptions import InvalidProcess

from prompt.mining.process.eventlog import *
# logging.basicConfig(level=logging.DEBUG, format='%(filename)s:%(lineno)s %(message)s')
TIME_FORMAT = '%Y-%m-%d %H:%M:%S%f'


class LogTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(LogTest, self).__init__(*args, **kwargs)
        self.process = Process(_id="p1")
        self.case1 = Case("c1")
        self.case2 = Case( "c2")

    def test_log(self):
        log = Log()
        log.add_case(self.case1)
        log.add_case(self.case2)
        self.assertEqual(len(log.cases), 2)
        self.assertTrue(isinstance(log, Log))

    def test_process_log(self):
        p_log = Log()
        p_log.add_case(self.case1)
        p_log.add_case(self.case2)
        self.assertEqual(len(p_log.cases), 2)

    def test_process_log_2(self):
        p_log = Log([self.case1, self.case2])
        self.assertEqual(len(p_log.cases), 2)


if __name__ == '__main__':
    unittest.main()