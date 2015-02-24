import uuid
import logging


class IdObject(object):
    def __init__(self, _id=None):
        self.id = _id or uuid.uuid4()


class Process(IdObject):

    def __init__(self, _id=None):
        """

        :return:
        """
        super(Process, self).__init__(_id)
        self._activities = {}
        self.cases = []

    # def __eq__(self, other):
    #     if type(self) == type(other):
    #         logging.debug('self.activities %s', [a.name for a in self.activities])
    #         logging.debug('other.activities %s', [a.name for a in other.activities])
    #         logging.debug('self.cases %s', self.cases)
    #         logging.debug('other.cases %s', other.cases)
    #
    #         # return set(self.activities) == set(other.activities) and set(self.cases) == set(other.cases)
    #         return set(self.activities) == set(other.activities)
    #
    #     return False

    def add_activity(self, name, _id=None):
        if name in self._activities:
            return self._activities[name]
        
        activity = Activity(_id=_id, name=name, process=self)
        activity.process = self
        self._activities[name] = activity
        return activity

    def get_activity_by_name(self, name):
        return self._activities[name]

    @property
    def activities(self):
        return self._activities.values()

    # def add_case(self, case_id):
    #     case = Case(_id=case_id)
    #     case.process = self
    #     self.cases.append(case)
    #     return case


class ProcessInfo(object):

    def __init__(self, process=None, activities_number=0, cases_number=0, average_case_size=0, events_number=0,
                 activity_frequencies=None, event_frequencies=None):
        self.process = process
        self.activities_number = activities_number
        self.cases_number = cases_number
        self.average_case_size = average_case_size
        self.events_number = events_number
        self.activity_frequencies = activity_frequencies or []
        self.event_frequencies = event_frequencies or []


class Activity(IdObject):

    def __init__(self, name, process, _id=None):
        """

        :return:
        """
        super(Activity, self).__init__(_id)
        self.name = name
        self.activity_instances = []
        self.process = process

    # def __eq__(self, other):
    #     logging.debug('--------self.name', self.name)
    #     return True
    #     if type(self) == type(other):
    #         return True
    #
    #     return False

    def __str__(self):
        return self.name


class Case(IdObject):

    def __init__(self, process, _id=None):
        super(Case, self).__init__(_id)
        self.process = process
        self.activity_instances = []

    def add_activity_instance(self, activity,_id=None):
        activity_instance = ActivityInstance(_id=_id, case=self, activity=activity)
        self.activity_instances.append(activity_instance)
        activity.activity_instances.append(activity_instance)
        return activity_instance

    @property
    def events(self):
        events = []
        for act_instance in self.activity_instances:
            events.extend(act_instance.events)
        return events



    # def __eq__(self, other):
    #     if type(self) == type(other):
    #         logging.debug('self %s != other %s', self, other)
    #         logging.debug("self.events %s", [e.activity_instance.activity.name for e in self.events])
    #         logging.debug("other.events %s", [e.activity_instance.activity.name for e in other.events])
    #
    #         return set(self.activity_instances) == set(other.activity_instances) \
    #             and set(self.events) == set(other.events)
    #
    #
    #     return False


class ActivityInstance(IdObject):

    def __init__(self, activity, case=None, _id=None):
        super(ActivityInstance, self).__init__(_id)
        self.case = case
        self.activity = activity
        self.events = []

    def __eq__(self, other):
        if type(self) == type(other):
            try:
                for counter in xrange(len(self.events)):
                    assert self.events[counter] == other.events[counter]
            except AssertionError, e:
                logging.debug('self %s ! = other %s', self, other)
                return False
            return True
        else:
            return False

    def add_event(self, timestamp=None, resources=None,  attributes=None, _id=None):
        event = Event(self, timestamp, resources,  attributes, _id)
        self.events.append(event)
        return event


class Event(IdObject):

    def __init__(self, activity_instance, timestamp=None, resources=None,  attributes=None, _id=None):
        """

        :return:
        """
        super(Event, self).__init__(_id)
        self.attributes = attributes or []
        self.timestamp = timestamp
        self.resources = resources or []
        self.activity_instance = activity_instance

    @property
    def activity_name(self):
        return self.activity_instance.activity.name

    @property
    def case(self):
        return self.activity_instance.case

    def add_attribute(self, name, value):
        attr = Attribute(name=name, value=value, event=self)
        self.attributes.append(attr)
        return attr

    # def __eq__(self, other):
    #     if type(self) == type(other):
    #         try:
    #             for counter in xrange(len(self.attributes)):
    #                 assert self.attributes[counter] == other.attributes[counter]
    #             assert self.timestamp == other.timestamp
    #             assert self.resources == other.resources
    #         except AssertionError, e:
    #             logging.debug('self %s != other %s', self, other)
    #             return False
    #         return True
    #     else:
    #         return False

    def __str__(self):
        return "timestamp %s, resources %s" % (self.timestamp, self.resources)


class Attribute(IdObject):

    def __init__(self, _id=None, name=None, value=None, event=None):
        super(Attribute, self).__init__(_id)
        self.name = name
        self.value = value
        self.event = event

    def __eq__(self, other):
        if type(self) == type(other):
            try:
                assert self.name == other.name
                assert self.value == other.value
            except AssertionError, e:
                logging.debug('self %s != other %s', self, other)
                return False
            return True
        else:
            return False

    def __str__(self):
        return "name: %s, value %s" % (self.name, self.value)