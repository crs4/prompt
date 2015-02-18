from pymine.mining.process.network import Network
from pymine.mining.process.network import Node
from pymine.mining.process.network import Arc

class BPMNDiagram(Network):

    def __init__(self, label, process_name=None, process_desc=None,  process_participants=None):
        super(BPMNDiagram, self).__init__(label)
        self.process_id = 'PC_%s'%label
        self.process_name = process_name
        self.process_desc = process_desc
        self.process_participants = process_participants
        self.attributes = {}

    # def get_start_activity(self):
    #     for act in self.process_activities:
    #         if act.is_start:
    #             return act
    #
    #
    # def get_end_activities(self):
    #     end_acts = []
    #     for act in self.process_activities:
    #         if act.is_end:
    #             end_acts.append(act)
    #     return end_acts
    #
    #
    # def add_activity(self, act):
    #     #Check that no other activity has the same id
    #     for a in self.process_activities:
    #         if act.id == a.id:
    #             raise Exception("Cannot have two activities with the same id:%s", act.id)
    #     self.process_activities.append(act)
    #
    #
    # def add_transaction(self, tr):
    #     for t in self.process_transactions:
    #         if t.id == tr.id:
    #             raise Exception('A transaction with the same ID already exists')
    #         if t.from_act == tr.from_act and t.to_act == tr.to_act:
    #             raise Exception("The model always have a transaction connecting the same activities")
    #     if tr.from_act not in self.process_activities or tr.to_act not in self.process_activities:
    #         raise Exception("Cannot add transaction: unknown activity ")
    #     self.process_transactions.append(tr)
    #
    #
    # def connect_activities(self, act1, act2):
    #     for t in self.process_transactions:
    #         if t.from_act in (act1.id, act2.id) and t.to_act in (act1.id, act2.id):
    #             raise Exception("The activities %s %s are yet connected", (act1.id, act2.id))
    #     tr = Transaction()
    #     tr.from_act = act1
    #     tr.to_act = act2
    #     self.process_transactions.append(tr)
    #
    #
    # def get_transactions(self):
    #     return self.process_transactions
    #
    #
    # def get_activity_by_id(self, id):
    #     for a in self.process_activities:
    #         if a.id == ('AC_%s'%id):
    #             return a
    #     return None


class Activity(Node):
    def __init__(self, label, net, frequency=None, description=None,  event=None, type='activity' ):
        super(Activity, self).__init__(label, net, frequency)
        self.description = description
        self.event = event
        self.attributes = {}
        self.is_start = False
        self.is_end = False


class Transaction(Arc):
    def __init__(self, label, input_node, output_node, frequency=None, desc=None ):
        super(Transaction, self).__init__(label, input_node, output_node, frequency)
        self.desc = desc
        self.attributes = {}
        self.condition = None


class ParallelGateway(Node):
    pass


class ExclusiveGateway(Node):
    pass
