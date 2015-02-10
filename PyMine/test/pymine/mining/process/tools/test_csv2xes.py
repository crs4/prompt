__author__ = 'paolo'

import unittest, time

import pymine.mining.process.tools.csv2xes as csv2xes

class TestCsv2Xes(unittest.TestCase):

    def __init__(self, label):
        super(TestCsv2Xes, self).__init__(label)

    def test_appendCaseRow(self):
        self.assertTrue(True)

def suite():
    suite = unittest.TestSuite()
    suite.addTest(TestCsv2Xes('test_appendCaseRow'))
    return suite

if __name__ == '__main__':
    runner = unittest.TestCsv2Xes(verbosity=2)
    runner.run(suite())
