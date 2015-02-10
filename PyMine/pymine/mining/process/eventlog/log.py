__author__ = 'paolo'

from hashlib import md5

class Log(object):

    def __init__(self):
        self.processes = {}
        self.cases = {}
        self.events = {}
        self.activities = {}
        self.activity_instances = {}
        self.attributes = {}

    def __eq__(self, other):
        if type(self) == type(other):
            return self.__hash__() == other.__hash__()
        else:
            return False

    def __hash__(self):
        log = [self.processes,
               self.cases,
               self.events,
               self.activities,
               self.activity_instances,
               self.attributes]
        return md5(str(log)).hexdigest()

    def __str__(self):
        return str({'processes' : self.processes,
                    'cases' : self.cases,
                    'events' : self.events,
                    'activities' : self.activities,
                    'activity_instances': self.activity_instances,
                    'attributes' : self.attributes})
