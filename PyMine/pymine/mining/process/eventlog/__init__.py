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

    def add_activity(self, activity):
        if activity.name in self._activities:
            return self._activities[activity.name]

        activity.process = self
        self._activities[activity.name] = activity
        return activity

    def get_activity_by_name(self, name):
        return self._activities.get(name)

    @property
    def activities(self):
        return self._activities.values()

    def add_case(self, case):
        case.process = self
        self.cases.append(case)
        return case


class Activity(IdObject):

    def __init__(self, name, _id=None):
        """

        :return:
        """
        super(Activity, self).__init__(_id)
        self.name = name
        self.activity_instances = []
        self.process = None

    def __str__(self):
        return self.name

    def __hash__(self):
        return hash(self.name)

    def add_actity_instance(self, activity_instance):
        self.activity_instances.append(activity_instance)
        activity_instance.activity = self
        return activity_instance

class Case(IdObject):
    """
    It represents a sequence of :class:`Event<.Event>`
    """

    def __init__(self, _id=None):
        super(Case, self).__init__(_id)
        self.process = None
        self.activity_instances = []
        self.events = []

    def add_activity_instance(self, activity_instance):
        self.activity_instances.append(activity_instance)
        activity_instance.case = self
        return activity_instance

    @property
    def activity_list(self):
        activities = []
        for act_instance in self.activity_instances:
            activities.append(act_instance.activity.name)
        return activities

    def add_event(self, event):
        event.case = self
        self.events.append(event)
        return event

    def __str__(self):
        return str([e.name for e in self.events])

    def __repr__(self):
        return str(self)

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

    def __init__(self, _id=None):
        super(ActivityInstance, self).__init__(_id)
        self.case = None
        self.activity = None
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

    @property
    def label(self):
        return self._label

    def add_event(self, event):
        self.events.append(event)
        event.activity_instance = self
        return event


class Event(IdObject):

    def __init__(self, name=None, timestamp=None, resources=None, attributes=None, _id=None,
                 activity_instance=None, case=None):
        """

        :param name:
        :param timestamp: a datetime
        :param activity_instance:
        :param resources: a list of objects
        :param attributes: a list of :class:`Attribute<pymine.mining.process.eventlog.Attribute>`
        :param _id: a unique id
        :return:
        """
        super(Event, self).__init__(_id)
        self.name = name
        self.case = case
        self.attributes = attributes or []
        self.timestamp = timestamp
        self.resources = resources or []
        self.activity_instance = activity_instance

    @property
    def activity(self):
        return self.activity_instance.activity if self.activity_instance else None

    def add_attribute(self, name, value):
        attr = Attribute(name=name, value=value, event=self)
        self.attributes.append(attr)
        return attr

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


class ProcessInfo(object):

    def __init__(self, process):

        self.process = process
        self.activities_number = len(process.activities)
        self.cases_number = len(process.cases)
        events_number = 0
        for case in process.cases:
            events_number += len(case.events)
        self.events_number = events_number
        self.average_case_size = float(events_number/len(process.cases))
        # self.activity_frequencies = activity_frequencies or []
        # self.event_frequencies = event_frequencies or []