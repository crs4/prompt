__author__ = 'paolo'

import datetime
from hashlib import md5
import uuid

class Event():

    def __init__(self, id=None, case_id=None, activity_instance_id=None, attributes=None):
        """

        :return:
        """
        if id:
            self.id = id
        else:
            self.id = uuid.uuid4()

        if case_id:
            self.case_id = case_id
        else:
            self.case_id = None

        if activity_instance_id:
            self.activity_instance_id = activity_instance_id
        else:
            self.activity_instance_id = None

        if attributes:
            self.attributes = attributes
        else:
            self.attributes = []

    def __str__(self):
        doc = "id="+str(self.id)+"\n" \
        "case_id="+str(self.case_id)+"\n" \
        "activity_instance_id="+str(self.activity_instance_id)+"\n" \
        "attributes="
        for attr in self.attributes:
            doc += str(attr)+"\n"
        return doc