from abc import abstractmethod
import socket


def get_free_port(port=1024, max_port=65535):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    while port <= max_port:
        try:
            sock.bind(('', port))
            sock.close()
            return port
        except OSError:
            port += 1
    raise IOError('no free ports')


class RpcerBase:
    def __init__(self, fmt=None):
        self.fmt = fmt

    @abstractmethod
    def send(self, target, msg):
        pass
    
    @property
    def addr(self):
        return self.server.addr
    
    @property
    def port(self):
        return self.server.port
        