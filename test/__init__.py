import unittest


def discover_suite(start_dir='.'):
    loader = unittest.TestLoader()
    return loader.discover('.')
