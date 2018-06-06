from secondary_functions import *
from subprocess import Popen, CalledProcessError, PIPE
from concurrent.futures import ThreadPoolExecutor
from itertools import combinations
from collections import Counter
import logging


class BTS_infrastructure(object):

    def __init__(self, port, servers, amount_of_enemies, size_of_QW, size_of_QR, right_file, wrong_file):
        self.INFRASTRUCTURE_PORT = port
        self.U = servers
        self.AMOUNT_OF_SERVERS = len(servers)
        self.AMOUNT_OF_ENEMIES = amount_of_enemies
        self.TS_FILE = right_file
        self.WRONG_TS_FILE = wrong_file
        self.THREAD_POOL_ON = True
        self.AMOUNT_OF_CLIENTS = 0
        self.QW = self.U[0:size_of_QW]
        self.QR = self.U[0:size_of_QR]   # [-size_of_QR:]
        self.ENTER = True
        logging.basicConfig(filename='infrastructure_log.txt', level=logging.INFO, format="%(asctime)s - %(message)s")
        logging.info('Infrastructure on port ' + str(self.INFRASTRUCTURE_PORT))

    def enter_r(self, pid, temp):
        while not self.ENTER:
            pass
        self.ENTER = False

        ts = {i: [None, None] for i in self.U}  # port: (socket, set)
        for i in self.U:
            ts[i][0] = connect_to_server(i)
            if ts[i][0] is not None:
                send_message(ts[i][0], {'op': 'enter', 'pid': pid, 'temp': temp})
                logging.info('ENTER_R: send ' + str(temp) + ' to server ' + str(i))
            else:
                ts.pop(i)
                logging.info('ENTER_R: server ' + str(i) + ' does not work')

        qr = set()
        for i in ts.keys():
            resp = get_message(ts[i][0])
            ts[i][0].close()
            if resp is None or resp['resp'] != 'go':
                logging.info('ENTER_R: wrong respond ' + str(resp) + ' from server ' + str(i))
                ts[i][1] = None
            else:
                ts[i][1] = resp['ts']
                qr.add(i)
        self.ENTER = True

        if not set(self.QR).issubset(qr):
            return tuple([None, None])

        return tuple([self.rdp(temp, ts), ts])

    def exit_r(self, pid, temp):
        for i in self.U:
            sock = connect_to_server(i)
            if sock is not None:
                send_message(sock, {'op': 'exit', 'pid': pid, 'temp': temp})
                sock.close()
            else:
                logging.info('EXIT_R: cannot exit cs with pid: ' + str(pid))

    def paxos(self, pid, temp, t, ts):
        logging.info('PAXOS: propose ' + str(t))
        for i in ts.keys():
            ts[i][0] = connect_to_server(i)
            if ts[i][0] is not None:
                send_message(ts[i][0], {'op': 'in', 'pid': pid, 'temp': temp, 'tup': t})

        tup_list = []
        for i in ts.keys():
            if ts[i][0] is not None:
                resp = get_message(ts[i][0])
                ts[i][0].close()
                if resp is not None:
                    tup_list.append(resp['resp'])
                else:
                    tup_list.append(None)

        c = Counter(tup_list)
        mc = c.most_common(1)[0]
        if mc[1] > self.AMOUNT_OF_ENEMIES:
            return mc[0]

        return None

    def out(self, t):
        for i in self.QW:
            sock = connect_to_server(i)
            if sock is not None:
                send_message(sock, {'op': 'out', 'tup': t})
                logging.info('OUT: send ' + str(t) + ' to server ' + str(i))
                sock.close()
            else:
                logging.info('OUT: cannot connect to server ' + str(i))
        logging.info('OUT: recorded ' + str(t))

    def rdp(self, temp, ts=None):
        if ts is None:
            ts = {i: [None, None] for i in self.QR}  # port: (socket, set)
            for i in self.QR:
                ts[i][0] = connect_to_server(i)
                if ts[i][0] is not None:
                    send_message(ts[i][0], {'op': 'rd', 'temp': temp})
                    logging.info('RDP: send ' + str(temp) + ' to server ' + str(i))
                else:
                    ts.pop(i)

            for i in ts.keys():
                msg = get_message(ts[i][0])
                ts[i][0].close()
                if msg is not None:
                    ts[i][1] = msg['ts']
                else:
                    ts[i][1] = None
                logging.info('RDP: receive ' + str(ts[i][1]) + ' from server ' + str(i))

        t = None
        for i in combinations(ts.keys(), self.AMOUNT_OF_ENEMIES + 1):
            ts_intersection = ts[i[0]][1]

            for j in i:
                if ts[j][1] is None:
                    ts_intersection = None
                    break

                ts_intersection = ts_intersection.intersection(ts[j][1])
                if len(ts_intersection) == 0:
                    break

            if ts_intersection is not None:
                if len(ts_intersection) > 0:
                    t = ts_intersection.pop()
                    break

        logging.info('RDP: selected ' + str(t))
        return t

    def inp(self, pid, temp):
        while True:
            t, ts = self.enter_r(pid, temp)
            logging.info('INP: enter with pid ' + str(pid) + ', temp ' + str(temp) + ', t: ' + str(t))

            if t is None:
                self.exit_r(pid, temp)
                logging.info('INP: exit from cs with pid ' + str(pid))
                return None

            d = self.paxos(pid, temp, t, ts)

            if d == t:
                break

        logging.info('INP: pid ' + str(pid) + ' deleted ' + str(t))
        return t

    def worker(self, client, pid):
        req = get_message(client)
        if req is not None:
            try:
                if req['op'] == 'out':
                    self.out(req['tup'])
                elif req['op'] == 'rd':
                    send_message(client, {'resp': self.rdp(req['temp'])})
                elif req['op'] == 'in':
                    send_message(client, {'resp': self.inp(pid, req['temp'])})
                elif req['op'] == 'stop':
                    self.THREAD_POOL_ON = False
                    self.stop_servers()
                    send_message(client, {'resp': 'ok'})
                else:
                    logging.info('WORKER: wrong request ' + str(req))
            except KeyError:
                logging.info('WORKER: wrong request ' + str(req))

            client.close()

    def start_servers(self, ind1, ind2, file):
        for i in range(ind1, ind2):
            try:
                Popen('python BTS_server.py ' + str(i) + ' ' + str(self.U[i]) + ' ' + str(file) + ' ' +
                      ''.join(str(j) + ' ' for j in self.U), stdout=PIPE, stderr=PIPE, shell=True)
            except CalledProcessError:
                logging.info('START_SERVERS: cannot start server on port ' + str(self.U[i]))
            else:
                logging.info('START_SERVERS: start server on port ' + str(self.U[i]))

    def stop_servers(self):
        for i in self.U:
            s = connect_to_server(i)
            if s is not None:
                send_message(s, {'op': 'stop'})
                resp = get_message(s)
                s.close()
                if resp['resp'] != 'ok':
                    logging.info('STOP_SERVERS: cannot stop server on port ' + str(i))
                else:
                    logging.info('STOP_SERVERS: stop server on port ' + str(i))
            else:
                logging.info('STOP_SERVERS: cannot stop server on port ' + str(i))

    def run(self):
        if self.AMOUNT_OF_ENEMIES >= self.AMOUNT_OF_SERVERS:
            return False

        self.start_servers(0, self.AMOUNT_OF_ENEMIES, self.WRONG_TS_FILE)
        self.start_servers(self.AMOUNT_OF_ENEMIES, self.AMOUNT_OF_SERVERS, self.TS_FILE)

        s = socket(AF_INET, SOCK_STREAM)
        s.bind(('', self.INFRASTRUCTURE_PORT))
        s.listen(1)

        with ThreadPoolExecutor(10) as pool:
            while self.THREAD_POOL_ON:
                try:
                    s.settimeout(30)
                    client_s, client_addr = s.accept()
                    self.AMOUNT_OF_CLIENTS += 1
                except error:
                    pass
                    # logging.error('RUN: socket.error while accepting')
                    # self.THREAD_POOL_ON = False
                else:
                    pool.submit(self.worker, client_s, self.AMOUNT_OF_CLIENTS)

        s.close()

        """
        while self.THREAD_POOL_ON:
            try:
                client_s, client_addr = s.accept()
                self.AMOUNT_OF_CLIENTS += 1
            except error:
                logging.error('RUN: socket.error in main while accepting')
                self.THREAD_POOL_ON = False
            else:
                self.worker(client_s, self.AMOUNT_OF_CLIENTS)

        s.close()
        """

        logging.info('RUN: stop working')
        return True
