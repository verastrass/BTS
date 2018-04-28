from secondary_functions import *
from subprocess import Popen, CalledProcessError, PIPE
from concurrent.futures import ThreadPoolExecutor
from itertools import combinations
import logging
# from OpenSSL import SSL
# from random import sample


class TupleSpace(object):

    def __init__(self, port, servers, amount_of_enemies, right_file, wrong_file):
        self.INFRASTRUCTURE_PORT = port
        self.U = servers
        self.AMOUNT_OF_SERVERS = len(servers)
        self.AMOUNT_OF_ENEMIES = amount_of_enemies
        self.TS_FILE = right_file
        self.WRONG_TS_FILE = wrong_file
        self.THREAD_POOL_ON = True
        self.AMOUNT_OF_CLIENTS = 0
        self.QW = self.U
        self.QR = self.U
        logging.basicConfig(filename='infrastructure_log.txt', level=logging.DEBUG, format="%(asctime)s - %(message)s")
        logging.info('Infrastructure on port ' + str(self.INFRASTRUCTURE_PORT))

    def enter_r(self, pid):
        sockets = []
        for i in self.U:
            sock = connect_to_server(i)
            if sock is not None:
                send_message(sock, {'op': 'enter', 'pid': pid})
                if i in self.QR:
                    sockets.append(sock)
                else:
                    sock.close()

        for i in sockets:
            resp = get_message(i)
            i.close()

            if resp['resp'] != 'go':
                logging.info('Wrong respond in enter_r while entering cs')
                # raise Exception('Wrong respond in enter_r')

    def exit_r(self, pid):
        for i in self.U:
            sock = connect_to_server(i)
            if sock is not None:
                send_message(sock, {'op': 'exit', 'pid': pid})
                sock.close()
            else:
                logging.info('Cannot exit cs with pid: ' + str(pid))

    def out(self, t):
        for i in self.QW:
            sock = connect_to_server(i)
            if sock is not None:
                send_message(sock, {'op': 'out', 'tup': t})
                logging.info('Send to server ' + str(i) + ' tup: ' + str(t))
                sock.close()
            else:
                logging.info('In out: cannot connect to server ' + str(i))

    def rdp(self, temp):
        ts = {i: [None, None] for i in self.U}  # port: (socket, set)
        for i in self.U:
            ts[i][0] = connect_to_server(i)
            if ts[i][0] is not None:
                send_message(ts[i][0], {'op': 'rd', 'temp': temp})
                logging.info('Send to server ' + str(i) + ' temp: ' + str(temp))

        qr = set()
        for i in self.U:
            if ts[i][0] is not None:
                ts[i][1] = get_message(ts[i][0])['ts']
                logging.info('Get from server ' + str(i) + ' ts: ' + str(ts[i][1]))
                ts[i][0].close()
                qr.add(i)
            else:
                ts[i][1] = None

            if qr.issubset(self.QR):
                break

        t = None
        for i in combinations(ts.keys(), self.AMOUNT_OF_ENEMIES + 1):
            ts_intersection = ts[i[0]][1]

            for j in i:
                ts_intersection = ts_intersection.intersection(ts[j][1])
                if len(ts_intersection) == 0:
                    break

            if len(ts_intersection) > 0:
                t = ts_intersection.pop()
                break

        return t

    def inp(self, pid, temp):
        while True:
            self.enter_r(pid)
            logging.info('Enter in cs with pid: ' + str(pid))
            t = self.rdp(temp)

            if t is None:
                self.exit_r(pid)
                logging.info('Exit from cs with pid: ' + str(pid))
                return None

            d = None
            # paxos(p, P, A, L, t)
            self.exit_r(pid)
            logging.info('Exit from cs with pid: ' + str(pid))

            if d == t:
                break

        return t

    def worker(self, client, pid):
        req = get_message(client)

        try:
            if req['op'] == 'out':
                self.out(req['tup'])
            elif req['op'] == 'rdp':
                send_message(client, {'resp': self.rdp(req['temp'])})
            elif req['op'] == 'inp':
                send_message(client, {'resp': self.inp(pid, req['temp'])})
            elif req['op'] == 'stop':
                self.THREAD_POOL_ON = False
                send_message(client, {'resp': 'ok'})
            else:
                logging.info('Wrong request: ' + str(req))
        except KeyError:
            logging.info('Wrong request: ' + str(req))

        client.close()

    def start_servers(self, ind1, ind2, file):
        for i in range(ind1, ind2):
            try:
                s = 'python server.py '+str(i)+' '+str(self.U[i])+' '+str(file)+' '+''.join(str(i)+' ' for i in self.U)
                logging.info(s)
                Popen('python server.py '+str(i)+' '+str(self.U[i])+' '+str(file)+' '+
                      ''.join(str(i)+' ' for i in self.U), stdout=PIPE, stderr=PIPE, shell=True)
            except CalledProcessError:
                logging.info("Cannot start server on port " + str(self.U[i]))
            logging.info("Started server on port " + str(self.U[i]))

    def stop_servers(self):
        for i in self.U:
            s = connect_to_server(i)
            if s is not None:
                send_message(s, {'op': 'stop'})
                s.close()
                logging.info("Stop server on port: " + str(i))
            else:
                logging.info("Cannot stop server on port: " + str(i))

    def run(self):
        if self.AMOUNT_OF_ENEMIES >= self.AMOUNT_OF_SERVERS:
            return False

        self.start_servers(0, self.AMOUNT_OF_ENEMIES - 1, self.WRONG_TS_FILE)
        self.start_servers(self.AMOUNT_OF_ENEMIES, self.AMOUNT_OF_SERVERS, self.TS_FILE)

        s = socket(AF_INET, SOCK_STREAM)
        s.bind(('', self.INFRASTRUCTURE_PORT))
        s.listen(1)
        # tls_s = wrap_socket(s, ssl_version=PROTOCOL_TLS_CLIENT)

        """
         with ThreadPoolExecutor(10) as pool:
            while self.THREAD_POOL_ON:
                try:
                    client_s, client_addr = s.accept()
                    self.AMOUNT_OF_CLIENTS += 1
                except error:
                    logging.error('socket.error in main while accepting')
                    self.THREAD_POOL_ON = False
                else:
                    pool.submit(self.worker, client_s, self.AMOUNT_OF_CLIENTS)
                    self.stop_servers()
        s.close()
        """
        while self.THREAD_POOL_ON:
            try:
                client_s, client_addr = s.accept()
                self.AMOUNT_OF_CLIENTS += 1
            except error:
                logging.error('socket.error in main while accepting')
                self.THREAD_POOL_ON = False
            else:
                self.worker(client_s, self.AMOUNT_OF_CLIENTS)

        s.close()

        return True
