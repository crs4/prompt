__author__ = 'paolo'

from pymine.mining.process.network.network import Network as Network
from pymine.mining.process.network.node import Node as Node
from pymine.mining.process.network.arc import Arc as Arc

class BPMNDiagram():

    def __init__(self, process_id=None, process_name=None, process_desc=None,  process_participants=None, process_activities=[], process_transactions=[]):
        #super(BPMNDiagram, self).__init__()
        self.process_id = 'PC_%s'%process_id
        self.process_name = process_name
        self.process_desc = process_desc
        self.process_participants = process_participants
        self.process_activities = process_activities
        self.process_transactions = process_transactions
        self.attributes = {}


    def get_start_activity(self):
        for act in self.process_activities:
            if act.is_start:
                return act


    def get_end_activities(self):
        end_acts = []
        for act in self.process_activities:
            if act.is_end:
                end_acts.append(act)
        return end_acts


    def add_activity(self, act):
        #Check that no other activity has the same id
        for a in self.process_activities:
            if act.id == a.id:
                raise Exception("Cannot have two activities with the same id:%s", act.id)
        self.process_activities.append(act)


    def add_transaction(self, tr):
        for t in self.process_transactions:
            if t.id == tr.id:
                raise Exception('A transaction with the same ID already exists')
            if t.from_act == tr.from_act and t.to_act == tr.to_act:
                raise Exception("The model always have a transaction connecting the same activities")
        if tr.from_act not in self.process_activities or tr.to_act not in self.process_activities:
            raise Exception("Cannot add transaction: unknown activity ")
        self.process_transactions.append(tr)


    def connect_activities(self, act1, act2):
        for t in self.process_transactions:
            if t.from_act in (act1.id, act2.id) and t.to_act in (act1.id, act2.id):
                raise Exception("The activities %s %s are yet connected", (act1.id, act2.id))
        tr = Transaction()
        tr.from_act = act1
        tr.to_act = act2
        self.process_transactions.append(tr)


    def get_transactions(self):
        return self.process_transactions


    def get_activity_by_id(self, id):
        for a in self.process_activities:
            if a.id == ('AC_%s'%id):
                return a
        return None


class Activity():
    def __init__(self, id=None, name=None, description=None,  event=None, type='activity' ):
        self.id = 'AC_%s'%id
        self.name = name
        self.description = description
        self.event = event
        self.attributes = {}
        self.is_start = False
        self.is_end = False
        self.type=type
        #super (Activity, self).__init__()


class Transaction():
    def __init__(self, id=None, name=None, desc=None, from_act=None, to_act=None):
        self.id = 'TR%s'%id
        self.name = name
        self.desc = desc
        self.from_act = from_act
        self.to_act = to_act
        self.attributes = {}
        self.condition = None
        #super(Transaction, self).__init__()


class Route():
    def __init__(self, type = 'routing' ):
        self.type = type
        #super(Route, self).__init__()













