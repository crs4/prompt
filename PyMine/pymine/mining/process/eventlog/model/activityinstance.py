__author__ = 'paolo'

import datetime
from hashlib import md5
import uuid

class ActivityInstance(object):

    def __init__(self, id=None, case_id=None, activity_id=None, events=None):

        if id:
            self.id = id
        else:
            self.id = uuid.uuid4()

        if case_id:
            self.case_id = case_id
        else:
            self.case_id = None

        if activity_id:
            self.activity_id = activity_id
        else:
            self.activity_id = None

        if events:
            self.events = events
        else:
            self.events = []