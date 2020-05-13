from Crypto.Hash import SHA256
import struct
import ecdsa_sig as sig
import request_pb2

def add_sig(key,id,seq,view,type,message,timestamp = None):
    #key = sig.get_signing_key(id)
    req = request_pb2.Request()
    inner = req.inner
    inner.id = id
    inner.seq = seq
    inner.view = view
    inner.type=type
    inner.msg = message
    if timestamp:
        inner.timestamp = timestamp
    b = inner.SerializeToString()
    h = SHA256.new()
    h.update(b)
    digest = h.digest()
    s = sig.sign(key, digest)
    req.dig = digest
    req.sig = s
    return req
   
def check(key,req):
    digest_recv = req.dig
    sig_recv = req.sig
    
    i = req.inner.SerializeToString()
    h = SHA256.new()
    h.update(i)
    digest = h.digest()
    
    s = (sig.verify(key, sig_recv, digest_recv) and digest == digest_recv)
    if s:
        return req
    else:
        return None


