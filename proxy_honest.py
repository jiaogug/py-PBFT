from __future__ import print_function
from builtins import str
from builtins import range
import proto_message as message
import config
import socket
import select, time
import sys, struct
import request_pb2
import threading

offset = 100
recv_mask = select.EPOLLIN | select.EPOLLPRI | select.EPOLLRDNORM
send_mask = select.EPOLLOUT | select.EPOLLWRNORM
fdmap = {}
buffmap = {}
outbuffmap = {}
lock = threading.Lock()
timeout = 1
logfile = open("proxy_log.txt", 'w', 1)
healed = False


def record(file, msg):
    file.write(str(time.time()) + "\t" + msg + "\n")


def record_req(file, req, port, immediate=True):
    dest = None
    for i in range(len(config.RL)):
        if port == config.RL[i][1]:
            dest = i
            break
    if immediate:
        msg = "Sending/"
    else:
        msg = ""
    msg += "Delivering a " + req.inner.type + " to replica "
    msg += str(dest) + " from replica " + str(req.inner.id)
    msg += " in view " + str(req.inner.view)
    record(file, msg)


def send_delay(ip, msg, req, port):
    global lock, logfile, offset
    lock.acquire()
    print("sending a delayed", req.inner.type, " message on port", port)
    time.sleep(0.01)
    sock = socket.socket()
    sock.connect((ip, port))
    rc = sock.send(serialize(req))
    # print "sent:",rc
    # print sock.getpeername()
    sock.close()
    record_req(logfile, req, port, False)
    lock.release()
    return


def serialize(req):
    size = req.ByteSize()
    b = struct.pack("!I", size)
    return b + req.SerializeToString()


def parse_request(request_bytes, fd):
    global timeout, healed
    req = request_pb2.Request()
    req.ParseFromString(request_bytes)
    id = req.inner.id
    # print "parsing a req from ", id, req.inner.type, req.inner.seq
    ip, port = portmap[fd]
    retry = True
    # print "attempting connection on", ip, port+offset
    count = 0
    lock.acquire()
    while retry:
        count += 1
        try:
            time.sleep(0.01)
            s = socket.socket()
            s.connect((ip, port))
            if len(outbuffmap[fd]) > 0:
                s.send(outbuffmap[fd])
                outbuffmap[fd] = ""
            s.send(serialize(req))
            # print(s.getpeername())
            if req.inner.type == "INIT":
                p.register(s, recv_mask)
                fdmap[s.fileno()] = s
                buffmap[s.fileno()] = ""
                portmap[s.fileno()] = config.RL[req.inner.id]
                outbuffmap[s.fileno()] = ""
            record_req(logfile, req, port)
        except:
            time.sleep(0.01)
            if count > 200:
                print("failed to send to", port)
                # raise
                break
            continue
        retry = False
    lock.release()

def clean(fd):
    p.unregister(fd)
    fdmap[fd].close()
    del fdmap[fd]
    del buffmap[fd]


if len(sys.argv) > 1:
    timeout = int(sys.argv[1])
print("Timeout:", timeout)
l_fd = []
p = select.epoll()
portmap = {}
for ip, port in config.RL:
    # listen on original ports
    s = socket.socket()
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    ip = '0.0.0.0'
    s.bind((ip, port))
    s.listen(50)
    s.setblocking(0)
    fdmap[s.fileno()] = s

    p.register(s)
    l_fd.append(s.fileno())
    print("Binding on %s:%d" % (ip, port))

print("starting loop")
while True:
    events = p.poll()
    for fd, event in events:
        if fd in l_fd:
            # print "GOT CON"
            c, addr = fdmap[fd].accept()
            p.register(c, recv_mask)
            fdmap[c.fileno()] = c
            buffmap[c.fileno()] = ""
            outbuffmap[c.fileno()] = ""
            portmap[c.fileno()] = (addr[0],fdmap[fd].getsockname()[1])
            #portmap[c.fileno()][0] = addr[0]
            print(portmap[c.fileno()])
        else:
            # read event
            if event & recv_mask != 0:
                try:
                    data = fdmap[fd].recv(100000)
                except:
                    raise
                if len(data) == 0:
                    # clean(fd)
                    pass
                buffmap[fd] += data
                while (len(buffmap[fd]) > 3):
                    # print "parsing...", len(buffmap[fd])
                    size = struct.unpack("!I", buffmap[fd][:4])[0]
                    if len(buffmap[fd]) >= size + 4:
                        parse_request(buffmap[fd][4:size + 4], fd)
                        buffmap[fd] = buffmap[fd][size + 4:]
                    else:
                        break
