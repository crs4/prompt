__author__ = 'paolo'

from hashlib import md5

class Activity(object):

    def __init__(self, id=None, name=None, activity_instances=None, process_id=None):
        """

        :return:
        """
        if id:
            self.id = id
        else:
            self.id = None

        if name:
            self.name = name
            if not self.id:
                self.id = name
        else:
            self.name = None

        if activity_instances:
            self.activity_instances = activity_instances
        else:
            self.activity_instances = []

        if process_id:
            self.process_id = process_id
        else:
            self.process_id = None