import communication
import time

def new_session(master_host, master_port, session_id):
    communication.init(b'fe'+session_id, "tcp://"+master_host+":"+master_port)
    communication.send(communication.TYPE_SESSION_BEGIN_PY)

def end_session():
    time.sleep(1)
    communication.send(communication.TYPE_SESSION_END_PY)
