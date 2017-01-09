import struct
import zmq

context = zmq.Context()
socket = zmq.Socket(context, zmq.DEALER)

TYPE_SESSION_BEGIN_PY = 0x3bfa1c58
TYPE_SESSION_END_PY = 0x2dfb1c58
NEW_TASK = 0x30fa1258
QUERY_TASK = 0x40fa1257

def init(identity, master_addr):
    global socket
    socket.setsockopt(zmq.IDENTITY, identity)
    socket.connect(master_addr)

def send(type, content=None):
    global socket
    socket.send('', zmq.SNDMORE)
    if content != None:
        socket.send(struct.pack('=i', type), zmq.SNDMORE)
        socket.send(content)
    else:
        socket.send(struct.pack('=i', type))

def ask(type, content=''):

    global socket
    socket.send('', zmq.SNDMORE)
    socket.send(struct.pack('=i', type), zmq.SNDMORE)
    socket.send(content)

    socket.recv()
    return socket.recv()
