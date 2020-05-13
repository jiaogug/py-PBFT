from builtins import str
from builtins import range
from builtins import object
import proto_message as message


class bank(object):
    def __init__(self,id,number):
        self.balance = 0
        self.number = number
        self.id = id
        self.total_tx = 0
        self.transaction_history = {}

    def process_request(self, key, id, seq, req, view):
        self.transaction_history[seq] = req
        self.total_tx += 1
        msg = req.inner.msg
        src = req.inner.id
        type = msg[:4]
        if not type in ["DEPT", "WDRL"]:
            m = message.add_sig(key, id, seq, view, "RESP", "INVALID", req.inner.timestamp)
            return m
            
        amount = int(msg[4:12])
        if type == "WDRL": # withdraw
            if amount > self.balance:  # we do not want the account to be negative
                amount = self.balance
            self.balance = self.balance - amount
            m = message.add_sig(key, id, seq, view, "RESP", "APPROVED", req.inner.timestamp)
            return m

        if type == "DEPT":
            if amount > 100: # we do not support more than 100 usd deposit at a time
                m = message.add_sig(key, id, seq, view, "RESP", "INVALID", req.inner.timestamp)
                # print "transfer request invalid"
                return m
            else:
                self.balance = self.balance + amount
                m = message.add_sig(key, id, seq, view, "RESP", "APPROVED", req.inner.timestamp)
                # print "transfer request approved"

                return m





    def print_balances(self):
        pass
        # come on, write your own debugger!


