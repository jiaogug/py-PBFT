# py-PBFT
Sample implementation of PBFT consensus algorithm.

It consists of two parts, workflow in positive network and the View-Change module for re-selection of leader.

# Usage
### node side
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

- step2: modify node.py

modify line 212: remote_ip into your proxy server
```
self.p.register(r, recv_mask)
retry = True
remote_ip = 'xxx.xxx.xxx.xxx'
```

- step3: make keys

run command as ` python make_keys.py N`

set N for number of replicas

- step4: start replicas
example for N == 4
```
python ./server.py 0 1000
python ./server.py 1 1000
python ./server.py 2 1000
python ./server.py 3 1000
```

## proxy side
- start proxy server

`python ./proxy_evil.py`
