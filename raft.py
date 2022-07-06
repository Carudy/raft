import threading
import time
import random
from loguru import logger

from rpcer import *


class RaftNode:
    def __init__(self, name):
        self.name = name
        self.comm = Grpcer(fmt='yaml')
        self.comm.start_server(callback=self.callback)
        self.term = 0
        self.get_ready()

    def get_ready(self):
        self.state = 'follower'
        self.voted = None
        self.votes = 0
        self.followed = None

    def callback(self, msg):
        # logger.info(f'{self.name} receive from {msg["name"]}')
        if msg['state'] == 'candidate':
            if self.state == 'follower' and self.voted is None:
                self.voted = msg['name']
                # logger.debug(f'Node {self.name} vote for {msg["name"]}.')
                return {'code': 'ok'}

        elif msg['state'] == 'leader':
            if self.state != 'leader' and self.followed is None:
                self.followed = msg['name']
                # logger.debug(f'Node {self.name} follow {msg["name"]}.')
                return {'code': 'ok'}

        return {'code': 'no'}

    def compete(self, others):
        self.get_ready()
        bound = (len(others)+1) * 0.5
        t = random.random() * 10
        # logger.debug(f'Node {self.name} sleep {t}s.')
        time.sleep(t)
        if self.voted is not None or self.followed is not None:
            return
        self.voted = self.name
        self.state = 'candidate'
        i = 0
        while i < len(others):
            if self.followed:
                break
            node = others[i]
            res = node.callback({
                'cmd': 'ask',
                'name': self.name,
                'state': self.state,
            })
            if res['code'] == 'ok':
                self.votes += 1
            if self.votes >= bound and self.state == 'candidate':
                logger.debug(f'Node {self.name} become a leader.')
                self.state = 'leader'
                i = -1
            i += 1


def start_raft(nodes):
    n_fail = 0
    for _ in range(5):
        tds = [threading.Thread(target=nodes[i].compete, kwargs={'others': [node for node in nodes if node.name != nodes[i].name]})
               for i in range(len(nodes))]
        for td in tds:
            td.start()
        for td in tds:
            td.join()
        res = [node for node in nodes if node.state == 'leader']
        if len(res) == 1:
            logger.info(f'Node {res[0].name} win, failed {n_fail} times.')
            return res[0]
        else:
            n_fail += 1
            logger.warning('Failed. Will retry.')
            _nodes = sorted(nodes, key=lambda x:-x.votes)
            for node in _nodes[:3]:
                logger.info(f'{node.name} get {node.votes}, is {node.state}')


if __name__ == '__main__':
    n = 1000
    nodes = [RaftNode(i) for i in range(n)]
    logger.level = 'info'
    start_raft(nodes)
