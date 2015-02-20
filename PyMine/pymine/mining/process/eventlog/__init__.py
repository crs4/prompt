import uuid


class IdObject(object):
    def __init__(self, _id = None):
        self.id = id or uuid.uuid4()


class Process(IdObject):

    def __init__(self, _id=None):
        """

        :return:
        """
        super(Process, self).__init__(_id)
        self.activities = []
        self.cases = []

    def __eq__(self, other):
        if type(self) == type(other):
            try:
                for counter in xrange(len(self.activities)):
                    assert self.activities[counter] == other.activities[counter]
                for counter in xrange(len(self.cases)):
                    assert self.cases[counter] == other.cases[counter]
            except AssertionError, e:
                return False
            return True
        else:
            return False

    def add_activity(self, activity_name, _id=None):
        activity = Activity(_id=_id, name=activity_name)
        activity.process = self
        self.activities.append(activity)
        return activity

    def add_case(self, case_id):
        case = Case(_id=case_id)
        case.process = self
        self.cases.append(case)
        return case


class ProcessInfo(object):

    def __init__(self, process=None, activities_number=0, cases_number=0, average_case_size=0, events_number=0,
                 activity_frequencies=[], event_frequencies=[]):
        self.process = process
        self.activities_number = activities_number
        self.cases_number = cases_number
        self.average_case_size = average_case_size
        self.events_number = events_number
        self.activity_frequencies = activity_frequencies
        self.event_frequencies = event_frequencies


class Activity(IdObject):

    def __init__(self, _id=None, name=None, activity_instances=[], process=None):
        """

        :return:
        """
        super(Activity, self).__init__(_id)
        self.name = name
        self.activity_instances = activity_instances
        self.process = process

    def __eq__(self, other):
        if type(self) == type(other):
            try:
                for counter in xrange(len(self.activity_instances)):
                    assert self.activity_instances[counter] == other.activity_instances[counter]
                assert self.name == other.name
            except AssertionError, e:
                return False
            return True
        else:
            return False


class Case(IdObject):

    def __init__(self, _id=None, process=None, activity_instances=[], events=[]):
        super(Case, self).__init__(_id)
        self.process = process
        self.activity_instances = activity_instances
        self.events = events

    def add_activity_instance(self, activity, _id=None):
        actvivity_instance = ActivityInstance(_id=_id, case=self, activity=activity)
        self.activity_instances.append(actvivity_instance)
        return actvivity_instance

    def add_event(self, activity_instance, _id=None, timestamp=None, resources=[], attributes=[]):
        event = Event(_id=_id, case=self, activity_instance=activity_instance,
                      timestamp=timestamp, resources=resources, attributes=attributes)
        activity_instance.events.append(event)
        self.events.append(event)
        return event

    def __eq__(self, other):
        if type(self) == type(other):
            try:
                for counter in xrange(len(self.activity_instances)):
                    assert self.activity_instances[counter] == other.activity_instances[counter]
                for counter in xrange(len(self.events)):
                    assert self.events[counter] == other.events[counter]
            except AssertionError, e:
                return False
            return True
        else:
            return False


class ActivityInstance(IdObject):

    def __init__(self, _id=None, case=None, activity=None, events=[]):
        super(ActivityInstance, self).__init__(_id)
        self.case = case
        self.activity = activity
        self.events = events

    def __eq__(self, other):
        if type(self) == type(other):
            try:
                for counter in xrange(len(self.events)):
                    assert self.events[counter] == other.events[counter]
            except AssertionError, e:
                return False
            return True
        else:
            return False


class Event(IdObject):

    def __init__(self, _id=None, case=None, activity_instance=None, timestamp=None, resources=[],  attributes=[]):
        """

        :return:
        """
        super(Event, self).__init__(_id)
        self.case = case
        self.activity_instance = activity_instance
        self.attributes = attributes
        self.timestamp = timestamp
        self.resources = resources

    def add_attribute(self, name, value):
        atr = Attribute(name=name, value=value, event=self)
        self.attributes.append(atr)
        return atr

    def __eq__(self, other):
        if type(self) == type(other):
            try:
                for counter in xrange(len(self.attributes)):
                    assert self.attributes[counter] == other.attributes[counter]
                assert self.timestamp == other.timestamp
                assert self.resources == other.resources
            except AssertionError, e:
                return False
            return True
        else:
            return False


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
                return False
            return True
        else:
            return False