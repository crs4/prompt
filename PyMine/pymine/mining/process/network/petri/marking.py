__author__ = 'paolo'

class Marking(object):

    def __init__(self):
        # Dictionaty describing the number of tokens in each place.
        # The key is the place while the value represents the number of tokens in the place
        self.places = {}