from time import sleep
from random import sample
from json import load, loads
from datetime import datetime
from types import SimpleNamespace
from argparse import ArgumentParser
from socket import AF_INET, SOCK_STREAM, socket
from selectors import DefaultSelector, EVENT_READ, EVENT_WRITE
import asyncio

from messagelist import MessageList

# parse command line arguments
parser = ArgumentParser()
parser.add_argument('-ip', type=str, required=True)
parser.add_argument('-port', type=int, required=True)
args = parser.parse_args()


# graph formed during the process will always be connected (induction)
# number (n/2)+1 is critical for connectedness of the graph

class PeerNode():
    """
    Peer Node class for functions related to peernode
    """

    def __init__(self, ip, port):
        """
        peers_list == set of (ip, port)
        connected_peers_list == dictionary = {(ip, port): (reader, writer)} ## TODO :outstanding messages??
        seeds_list == list f dict; each dict == info of a seed = {'ip':___ , 'port': ___, 'reader_writer': (reader, writer)}; only connected seeds
        """
        self.ip = ip
        self.port = port
        # We have handle_client() in place of this.
        # self.initialize_master_socket()
        self.peers_list = set()
        self.seeds_list = {}
        self.connected_peers_list = {}
        # Might not need it here. Shift it to handle_client
        # self.register_with_seeds()
        # self.connect_to_peers()
        self.gossip_message_number = 1
        self.ML = MessageList()
        # start periodic execution
        # self.start_periodic_functions()

# PEER_STATE = {}

    def register_with_seeds(self):
        """
        read seed info from file and register with n//2+1 seeds
        ## explicit register message required because incoming conn port is not same as conn port
        """
        seeds_info = []
        with open('seeds_info.txt', 'r') as info_fd:
            seeds_info = load(info_fd)

        seeds_to_register = sample(seeds_info, len(seeds_info)//2+1)
        for seed in seeds_to_register:
            seed_reader, seed_writer = asyncio.open_connection(seed['ip'], seed['port'])
            register_message = ("Register:%s:%d" % (self.ip, self.port)).encode()
            seed_writer.write(str(len(register_message)).zfill(8).encode() + register_message)
            await seed_writer.drain()

            self.get_and_parse_peer_list(s)
            seed['reader_writer'] = (seed_reader, seed_writer)
        self.seeds_list = seeds_to_register

    def get_and_parse_peer_list(self, reader):
        """
        given a seed reader get the peer list and add to self.peers_list
        format of received seeds info is same as seeds_info.json
        ## Might not be compulsary here in asyncio recv exact length of peers_list otherwise blocking. CAN USE READER.READLINE
        """
        peer_list_length = int(reader.read(8))
        stream_data = []
        while peer_list_length > 0:
            fragment = reader.read(peer_list_length).decode()
            stream_data.append(fragment)
            peer_list_length -= len(fragment)

        peers_dict = loads(''.join(stream_data))
        for peer in peers_dict:
            self.peers_list.add((peer['ip'], peer['port']))
        print(self.peers_list)
        # TODO: Write the peers_list recieved from a seed into an output file.

    def connect_to_peers(self):
        """
        connect to  peers (at max 4) after getting peer list from all seeds
        """
        total_connected_peers = 0
        for peer in self.peers_list:
            try:
                peer_reader, peer_writer = asyncio.open_connection(peer[0], peer[1])

                data = SimpleNamespace(
                    addr=(peer[0], int(peer[1])), inb=b"", outb=b"")
                events = EVENT_READ | EVENT_WRITE
                self.selector.register(s, events, data=data)
                self.connected_peers_list.append([peer[0], peer[1], s, []])
                print("connected to peer with ip %s:%s" % (peer[0], peer[1]))
                total_connected_peers += 1
            except Exception as e:
                print(e)
                # unable to connect to current peer; do nothing
                print("Unable to connect to peer:", peer)
            finally:
                if total_connected_peers == 4:
                    break

    async def handle_client(reader, writer):

        # Get peer's list from all the seeds
        await register_with_seeds()

        # 1st connect to at most 4 peers
        await connect_to_peers()

        # Handeling incoming messages and replying appropriately

        ip_port = writer.get_extra_info('peername') #tuple (ip,port)
        self.peer_list[ip_port] = reader, writer
        PEER_STATE[ip_port] = 0
        while True:
            request_message = await reader.readline()
            if request_message.startswith(b'gossip'):
                await gossip_forward(request_message)
            elif request_message.startswith(b'reply'):
                PEER_STATE[ip_port] = 0
            elif request_message.startswith(b'liveness'):
                await liveness_reply(request_message, writer)
            elif request_message == b'':
                print("band ho gaya bhosdiwala {}".format(ip_port))
                writer.close()
                await writer.wait_closed()
                self.peer_list.pop(ip_port)
            else:
                writer.write(b'samhje nahi, dubara boliye')
                await writer.drain()
        #     print("band ho gaya bhosdiwala {}".format(ip_port))
        #     self.peer_list.pop(ip_port)

    async def gossip_forward(message):
        if message not in self.ML:
            self.ML.add(message)
        # forwarding to all as of now, otherwise parse the incoming one and exclude it
            for peer in self.peer_list:
                reader, writer = self.peer_list[peer]
                writer.write(message)
                await writer.drain()

    async def liveness_reply(message,writer):
        writer.write(message)
        await writer.drain()

    async def liveness_send():
        while True:
            try:
                print("connected peers:",self.peer_list.keys())
                await asyncio.sleep(6)
                for peer in self.peer_list:
                    if PEER_STATE[peer] >=3:
                        print("Ma chuda le peer {}".format(peer)) 
                        self.peer_list.pop(peer)
                        writer.close()
                        await writer.wait_closed()
                    reader, writer = self.peer_list[peer]
                    writer.write(b"lelele zinda hai?\n")
                    await writer.drain()
                    PEER_STATE[peer]+=1
                    print("liveness bhej di bc {} {}".format(peer, PEER_STATE[peer]))
            except KeyError:
                print("hmmm")

    async def gossip_send():
        while self.gossip_message_number<10:
            if self.peer_list:
                i+=1
                print("kar dali chugli #{}".format(i))
            for peer in self.peer_list:
                reader, writer = self.peer_list[peer]
                writer.write(b"bc bc chugli\n")
                await writer.drain()
            await asyncio.sleep(5)
        print("Kar daali saari chugli bc")

    def run(self):

        self.loop = asyncio.get_event_loop()
        self.loop.create_task(asyncio.start_server(handle_client, self.ip, self.port))
        self.loop.create_task(liveness_send())
        self.loop.create_task(gossip_send())
        self.loop.run_forever()

peer = PeerNode(args.ip, args.port)
peer.run()
