import unittest
from test import discover_suite

# TODO: move all tests in __init__ files to files named test_* otherwise they will not be discovered

if __name__ == '__main__':
    suite = discover_suite()
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)