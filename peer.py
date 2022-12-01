from time import sleep
from random import sample
from hashlib import sha256
from json import load, loads
from threading import Thread
from datetime import datetime
from time import time as utctime
from types import SimpleNamespace
from argparse import ArgumentParser
from socket import AF_INET, SOCK_STREAM, socket
from selectors import DefaultSelector, EVENT_READ, EVENT_WRITE

# parse command line arguments
parser = ArgumentParser()
parser.add_argument('-ip', type=str, required=True)
parser.add_argument('-port', type=int, required=True)
parser.add_argument(
    '--liveness',
    help="Enable logging of liveness messages",
    action="store_true")
parser.add_argument(
    '--outputfile', help="name of output file", default='outputpeer.txt')
parser.add_argument(
    '--time',
    help="Specify time format to be used",
    choices=[
        'UNIX',
        'UNIX_T',
        'HUMAN'],
    default='UNIX_T')
parser.add_argument(
    '--report-ports',
    help="Report ports along with IP addresses",
    action="store_true")
args = parser.parse_args()

LOG_LIVENESS = args.liveness
TIME_FORMAT = args.time
USE_PORTS = args.report_ports


def time():
    if TIME_FORMAT == 'UNIX':
        return utctime()
    elif TIME_FORMAT == 'UNIX_T':
        return int(utctime())
    elif TIME_FORMAT == 'HUMAN':
        return str(datetime.utcnow()).replace(":", "-")

# graph formed during the process will always be connected (induction)
# number (n/2)+1 is critical for connectedness of the graph


# exactly receive length bytes from sock
def recv_all(sock, length):
    recv_data = []
    while length > 0:
        try:
            fragment = sock.recv(length)
            if not fragment:
                # connection closed by user
                return False
            length -= len(fragment)
            recv_data.append(fragment)
        except ConnectionResetError:
            return False
    return b''.join(recv_data)


def send_format(message: bytes):
    return str(len(message)).zfill(8).encode() + message


class MessageList():
    """
    More fields will be added as per requirements
    """

    def __init__(self):
        self.message_dict = {}

    def put(self, message_hash):
        self.message_dict[message_hash] = True

    def get(self, message_hash):
        if message_hash in self.message_dict:
            return True
        else:
            return False


class PeerNode():
    """
    Peer Node class for functions related to peernode
    """

    def __init__(self, ip, port):
        """
        peer list format here is (ip, port)
        connected peer list is of form [ip, port, socket, outstanding messages]
        seed list format is [ip, port, socket]; only connected seeds
        """
        self.ip = ip
        self.port = port
        self.log_file = open(args.outputfile, 'ab')
        self.initialize_master_socket()
        self.peers_list = set()
        self.seeds_list = []
        self.connected_peers_list = []
        self.register_with_seeds()
        self.connect_to_peers()
        self.gossip_message_number = 1
        self.ML = MessageList()
        # start periodic execution
        self.start_periodic_functions()

    def log(self, message):
        """
        log the message to the logfile and to screen
        """
        if isinstance(message, (str)):
            print(message)
            self.log_file.write((message + '\n').encode())
        elif isinstance(message, (bytes, bytearray)):
            print(message.decode())
            self.log_file.write(message + b'\n')

    def start_periodic_functions(self):
        gossip_thread = Thread(target=self.send_gossip_message)
        liveness_thread = Thread(target=self.send_liveness_request)
        gossip_thread.start()
        liveness_thread.start()

    def initialize_master_socket(self):
        """
        Initializes the socket and selector object
        """
        self.selector = DefaultSelector()
        self.master_socket = socket(AF_INET, SOCK_STREAM)
        self.master_socket.bind((self.ip, self.port))
        self.master_socket.listen()
        self.master_socket.setblocking(False)
        self.selector.register(self.master_socket, EVENT_READ, data=None)

    def get_and_parse_peer_list(self, s):
        """
        given a seed socket get the peer list and add to self.peers_list
        format of received seeds info is same as seeds_info.json
        recv exact length of peers_list otherwise blocking
        """
        peer_list_length = int(recv_all(s, 8).decode())
        peers_dict = loads(recv_all(s, peer_list_length).decode())
        seed_host, seed_port = s.getpeername()
        if USE_PORTS:
            self.log("Peer list from seed {} {}".format(seed_host, seed_port))
        else:
            self.log("Peer list from seed {}".format(seed_host))
        for peer in peers_dict:
            self.peers_list.add((peer['ip'], peer['port']))
            self.log("{}:{}".format(peer['ip'], peer['port']))
        self.log("")  # just an additional newline as a separator

    def register_with_seeds(self):
        """
        read seed info from file and register with n/2+1 seeds
        explicit register message required because incoming conn port is not same as conn port
        """
        seeds_info = []
        with open('config.txt', 'r') as info_fd:
            seeds_info = load(info_fd)

        seeds_to_register = sample(seeds_info, len(seeds_info) // 2 + 1)
        for seed in seeds_to_register:
            s = socket(AF_INET, SOCK_STREAM)
            s.connect((seed['ip'], seed['port']))
            register_message = ("Register:%s:%d" %
                                (self.ip, self.port)).encode()
            s.sendall(send_format(register_message))
            # s.sendall(str(len(register_message)).zfill(
            # 8).encode() + register_message)
            self.get_and_parse_peer_list(s)
            seed['socket'] = s
        # print(self.peers_list)
        self.seeds_list = seeds_to_register

    def connect_to_peers(self):
        """
        connect to  peers (at max 4) after getting peer list from all seeds
        """
        total_connected_peers = 0
        for peer in self.peers_list:
            try:
                s = socket(AF_INET, SOCK_STREAM)
                s.connect((peer[0], int(peer[1])))
                # newly connected peer will need listening port of this peer;
                # (max port is 65535)
                s.sendall(str(self.port).encode().zfill(8))
                data = SimpleNamespace(
                    addr=(peer[0], int(peer[1])), inb=b"", outb=b"")
                events = EVENT_READ | EVENT_WRITE
                self.selector.register(s, events, data=data)
                self.connected_peers_list.append([peer[0], peer[1], s, []])
                #print("connected to peer with ip %s:%s" % (peer[0], peer[1]))
                total_connected_peers += 1
            except:
                # unable to connect to current peer; do nothing
                print("Unable to connect to peer:", peer)
            finally:
                if total_connected_peers == 4:
                    break

    def report_peer_death(
            self,
            dead_peer_ip,
            dead_peer_port,
            current_timestamp):
        """
        sends all seeds the message that peer is dead ):
        """
        dead_message = ("Dead Node:%s:%s:%s:%s" % (
            dead_peer_ip, dead_peer_port, current_timestamp, self.ip))
        if USE_PORTS:
            dead_message += '_{}'.format(self.port)
        self.log("Reporting Dead node message " + dead_message)
        dead_message = dead_message.encode()
        for seed in self.seeds_list:
            seed['socket'].sendall(send_format(dead_message))
            # str(len(dead_message)).zfill(8).encode()+dead_message)

    def send_liveness_request(self):
        """
        this function will be executed periodically
        may have to move this function outside of the class
        TODO: try catch 'send' (what happen if connection close and send)
        when a message is liveness reply comes
        remove all timestamps which have lesser value then the received one
        """
        #print("sending liveness request")
        current_timestamp = time()
        request_message = ("Liveness Request:%s:%s" % (
            current_timestamp, self.ip)).encode()
        if USE_PORTS:
            request_message += '_{}'.format(self.port).encode()
        for peer in self.connected_peers_list:
            # if there are already 3 outstanding messages in list then peer is
            # ded
            if len(peer[3]) >= 3:
                #print("report death of peer %s:%s" % (peer[0], peer[1]))
                self.report_peer_death(peer[0], peer[1], current_timestamp)
                peer[2].close()
                self.connected_peers_list.remove(peer)
            else:
                # if send fails then peer closed the connection in middle
                try:
                    peer[2].sendall(send_format(request_message))
                    # str(len(request_message)).zfill(8).encode() +
                    # request_message)
                except:
                    pass
                peer[3].append(request_message)
        sleep(13)
        self.send_liveness_request()

    def send_gossip_message(self):
        """
        this function will also be executed periodically (10 times)
        send gossip message to all connected peers
        The first message should be generated by a peer,
        after it connects to selected neighbors after registration.
        """
        #print("sending gossip message")
        current_timestamp = time()
        if USE_PORTS:
            gossip_message = ("%s:%s:%s" % (current_timestamp, self.ip +
                                            "_" + str(self.port), self.gossip_message_number)).encode()
        else:
            gossip_message = (
                "%s:%s:%s" %
                (current_timestamp,
                 self.ip,
                 self.gossip_message_number)).encode()
        if len(self.connected_peers_list) > 0:
            self.gossip_message_number += 1
        for peer in self.connected_peers_list:
            try:
                peer[2].sendall(send_format(gossip_message))
                # str(len(gossip_message)).zfill(8).encode() + gossip_message)
            except:
                pass
        # a message sent by peer can come back (when new node joins the
        # network)
        self.ML.put(sha256(gossip_message).hexdigest())
        if self.gossip_message_number > 10:
            return
        sleep(5)
        self.send_gossip_message()

    def accept_wrapper(self, sock):
        """
        incoming connection from new node
        """
        try:
            conn, addr = sock.accept()
            listening_port = int(recv_all(conn, 8).decode())
            #print("Accepted connection from %s:%s" % (addr[0], addr[1]))
            self.peers_list.add(addr)
            self.connected_peers_list.append(
                (addr[0], listening_port, conn, []))
            data = SimpleNamespace(addr=addr, inb=b"", outb=b"")
            events = EVENT_READ | EVENT_WRITE
            self.selector.register(conn, events, data=data)
        except Exception as e:
            print(e)

    def service_connection(self, key, mask):
        """
        Service a connection request
        Three types of incoming messages and their action
        1. liveness request: send liveness reply
        2. liveness reply: modify outstanding message list of connected peer
        4. gossip: check if already received; if no then ml entry and forward to rest of the peers
        """
        sock = key.fileobj
        if mask & EVENT_READ:
            message_len = recv_all(sock, 8)
            if message_len:
                message_len = int(message_len.decode())
                recv_data = recv_all(sock, message_len)
                if not recv_data:
                    return
                splitted_message = recv_data.decode().split(":")
                # print(recv_data)
                if b'Liveness Request' in recv_data:
                    if LOG_LIVENESS:
                        self.log(recv_data)
                    liveness_reply = (
                        "Liveness Reply:%s:%s:%s" %
                        (splitted_message[1], splitted_message[2], self.ip)).encode()
                    if USE_PORTS:
                        liveness_reply += '_{}'.format(self.port).encode()
                    try:
                        sock.sendall(send_format(liveness_reply))
                        # str(len(liveness_reply)).zfill(8).encode()+liveness_reply)
                    except:
                        pass
                elif b'Liveness Reply' in recv_data:
                    self.log(recv_data)
                    for peer in self.connected_peers_list:
                        if peer[2] == sock:
                            # keep only those outstanding messages which have
                            # timestamp greater than reply
                            for msg in peer[3]:
                                if msg.decode().split(
                                        ':')[1] <= splitted_message[1]:
                                    peer[3].remove(msg)
                else:
                    message_hash = sha256(recv_data).hexdigest()
                    if not self.ML.get(message_hash):
                        self.ML.put(message_hash)
                        self.log('gossip_message:'+ recv_data.decode() +
                                 'local_timestamp:'+ str(datetime.now()))
                        for peer in self.connected_peers_list:
                            # forward to all peers except from which message
                            # has been received
                            if peer[2] != sock:
                                try:
                                    peer[2].sendall(
                                        str(message_len).zfill(8).encode() + recv_data)
                                except:
                                    pass
            else:
                # socket closed
                self.selector.unregister(sock)
                # close only after dead message report
                # sock.close()

    def run(self):
        """
        Main Loop
        """
        try:
            while True:
                events = self.selector.select(timeout=None)
                for key, mask in events:
                    if key.data is None:
                        # its from the listening socket, we need to accept
                        # the connection
                        self.accept_wrapper(key.fileobj)
                    else:
                        self.service_connection(key, mask)
        except KeyboardInterrupt:
            print("Exiting")
            self.log_file.close()
        finally:
            self.selector.close()


peer = PeerNode(args.ip, args.port)
peer.run()
