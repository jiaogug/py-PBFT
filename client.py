from __future__ import print_function
import request_pb2
import proto_message as message
import socket
import config
import sys, struct
import threading
import select
import time
import random

print(sys.argv)
id = int(sys.argv[1])
kill_flag = False
offset = 100
def recv_response():
    global kill_flag
    print("RECEIVING")
    s = socket.socket()
    p = select.epoll()
    ip,port = config.client[id]
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.setblocking(0)
    ip = '0.0.0.0'
    s.bind((ip,port))
    s.listen(50)
    p.register(s)
    f = open("client_log%d.txt" % id, 'w')
    f.write(str(time.time()) + " SEQUENCE: " + "0"  + " REPLICA: " + "START" + "\n")
    while(True):
        events = p.poll()
        for fd, event in events:
            c,addr = s.accept()
            r = c.recv(4096)
            size = struct.unpack("!I", r[:4])[0]
            req = request_pb2.Request()
            req.ParseFromString(r[4:])
            #print req.inner.msg, req.inner.seq, "FROM", req.inner.id
            f.write(str(time.time()) + " SEQUENCE: " + str(req.inner.seq) + " REPLICA: " + str(req.inner.id) + "\n")
            if req.inner.seq % 100 == 1:
                print("SEQUENCE:", req.inner.seq, "FROM", req.inner.id, "IN VIEW", req.inner.view)

import ecdsa_sig as sig
signing_key = sig.get_signing_key(id)

t = threading.Thread(target=recv_response)
t.daemon = True
t.start()

print("Loaded Messages")
print("Starting send")
sock_list = []
for ip,port in [config.RL[id]]:
    retry = True
    while retry:
        try:
            s = socket.socket()
            s.connect((ip,port+offset))
            sock_list.append(s)
            retry = False
        except Exception as e:
            time.sleep(0.5)
            s.close()
            print("trying to connect to %s : %d, caused by %s" % (ip, port, str(e)))

counter = 0

payload = open('client_%d.txt' % id, 'r').read().strip()

def prepare_client_requests():
    global counter
    counter += 1
    amount = random.randint(100, 300)
    msg = payload
    msg += msg * 1024
    key = signing_key
    temp = message.add_sig(key, id, counter + 1, 0, "REQU", msg, counter)
    size = temp.ByteSize()
    b = struct.pack("!I", size)
    return b + temp.SerializeToString()

while True:
    sock_list[0].send(prepare_client_requests())
    time.sleep(1)

