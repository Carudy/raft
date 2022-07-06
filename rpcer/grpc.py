from concurrent import futures
import grpc
import yaml

from .base import *
from .proto import *


class BytesService(bytes_pb2_grpc.Bytes):
    def __init__(self, master=None, fmt=None):
        self.master = master
        self.fmt = fmt
        self.msg_queue = []

    def send(self, request, context):
        msg = request.msg
        self.master.n_comm += 1
        if self.master.callback is not None:
            if self.fmt == 'yaml':
                msg = yaml.safe_load(msg)

            res = self.master.callback(msg)

            if self.fmt == 'yaml' and not isinstance(res, bytes):
                res = yaml.safe_dump(msg).encode()
            return bytes_pb2.Res(res=res)
        return bytes_pb2.Res(res=''.encode())


class BytesServer:
    def __init__(self, callback=None, addr='localhost', port=None, fmt=None):
        self.fmt = fmt
        self.callback = callback
        self.n_comm = 0
        self.addr = addr
        self.port = port

    def start_server(self):
        self.rpc_service = BytesService(master=self, fmt=self.fmt)
        self.rpc_server = grpc.server(
            futures.ThreadPoolExecutor(max_workers=4))
        bytes_pb2_grpc.add_BytesServicer_to_server(
            self.rpc_service, self.rpc_server)
        if self.port is None:
            self.port = get_free_port()
        self.rpc_server.add_insecure_port(f'[::]:{self.port}')
        self.rpc_server.start()


class BytesClient:
    def __init__(self, fmt=None):
        self.fmt = fmt
        self.conns = {}

    def send(self, target, msg):
        if isinstance(target, dict):
            addr, port = target['addr'], target['port']
        elif isinstance(target, (list, tuple)):
            addr, port = target
        elif isinstance(target, RpcerBase):
            addr, port = target.addr, target.port
        else:
            raise Exception('Unrecognizable target.')

        ap = f'{addr}:{port}'
        if ap not in self.conns:
            ch = grpc.insecure_channel(ap)
            self.conns[ap] = bytes_pb2_grpc.BytesStub(ch)
        if self.fmt == 'yaml':
            msg = yaml.safe_dump(msg).encode()

        res = self.conns[ap].send(bytes_pb2.Msg(msg=msg)).res

        if self.fmt == 'yaml':
            res = yaml.safe_load(res)

        return res


class Grpcer(RpcerBase):
    def __init__(self, fmt):
        super().__init__(fmt=fmt)
        self.client = BytesClient(fmt=self.fmt)

    def start_server(self, callback=None, port=None):
        self.server = BytesServer(callback=callback, port=port, fmt=self.fmt)
        self.server.start_server()

    def send(self, target, msg):
        return self.client.send(target, msg)
    