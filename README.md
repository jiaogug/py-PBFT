# py-PBFT
Sample implementation of PBFT consensus algorithm.

It consists of two parts, workflow in positive network and the View-Change module for re-selection of leader.

# Usage
### node
- step1: modify config.py

modify N into Number of replicas

`
N = 4
`

modify IP for nodes of PBFT network.
```
# REPLICA LIST
# IP, port
RL = []
RL.append(("node1", 30001))
RL.append(("node2", 30002))
RL.append(("node3", 30003))
RL.append(("node4", 30004))
```

- step2: modify 

\# TODO