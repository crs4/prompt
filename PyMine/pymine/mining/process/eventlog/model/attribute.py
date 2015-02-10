__author__ = 'paolo'

import datetime
from hashlib import md5
import uuid

class Attribute(object):

    def __init__(self, id=None, attribute_name=None, attribute_value=None, event_id=None):

        if id:
            self.id = id
        else:
            self.id = uuid.uuid4()

        if attribute_name:
            self.attribute_name = attribute_name
        else:
            self.attribute_name = None

        if attribute_value:
            self.attribute_value = attribute_value
        else:
            self.attribute_value = None

        if event_id:
            self.event_id = event_id
        else:
            self.event_id = None

    def __str__(self):
        return "attribute_id="+str(self.id)+" " \
        "attribute_name="+str(self.attribute_name)+" " \
        "attribute_value"+str(self.attribute_value)

class Timestamp(Attribute):

    def __init__(self, id=None, attribute_name=None, attribute_value=None, event_id=None):
        """

        :return:
        """
        super(Timestamp, self).__init__(id=id, attribute_name="timestamp", attribute_value=attribute_value, event_id=event_id)

class Resource(Attribute):

    def __init__(self, id=None, attribute_name=None, attribute_value=None, event_id=None):
        """

        :return:
        """
        super(Resource, self).__init__(id=id, attribute_name="resource", attribute_value=attribute_value, event_id=event_id)

