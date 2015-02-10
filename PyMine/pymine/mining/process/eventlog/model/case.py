__author__ = 'paolo'

import datetime
from hashlib import md5
import uuid

class Case():

    def __init__(self, id=None, process_id=None, activity_instances=None, events=[]):
        """

        :return:
        """
        if id:
            self.id = id
        else:
            self.id = uuid.uuid4()

        if process_id:
            self.process_id = process_id
        else:
            self.process_id = None

        if activity_instances:
            self.activity_instances = activity_instances
        else:
            self.activity_instances = []

        if events:
            self.events = events
        else:
            self.events = []

    def __str__(self):
        doc = "id="+self.id+"\n" \
        "process_id="+str(self.process_id)+"\n" \
        "cases_number="+str(self.cases_number)+"\n" \
        "activity_instances="+str(self.activity_instances)+"\n" \
        "events="+str(self.events)+"\n"
        return doc