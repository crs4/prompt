__author__ = 'paolo'

import datetime
from hashlib import md5
import uuid

class Process():

    def __init__(self, id=None, activities=None, cases=None):
        """

        :return:
        """
        if id:
            self.id = id
        else:
            self.id = uuid.uuid4()

        if activities:
            self.activities = activities
        else:
            self.activities = []

        if cases:
            self.cases = cases
        else:
            self.cases = []

    def __str__(self):
        doc = "process_id="+self.id+"\n" \
        "cases="+str(self.cases)+"\n" \
        "activities="+str(self.activities)
        return doc