from __future__ import print_function
import os
# This Replica ID
ID = 0

# Number of replicas
N = 4

# Number of failures we can tolerate
MAX_FAIL = 1

# REPLICA LIST
# IP, port
RL = []
RL.append(("node1", 30001))
RL.append(("node2", 30002))
RL.append(("node3", 30003))
RL.append(("node4", 30004))

client = [(r[0], r[1]+1000) for r in RL]

# KEY DIRECTORY
KD = os.getcwd() + "/keys"
print(KD)
