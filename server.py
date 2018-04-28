from secondary_functions import *
import argparse
from concurrent.futures import ThreadPoolExecutor
from collections import deque
import logging
# from OpenSSL import SSL


TS = set()
RS = set()
QR = deque()
SERVER_ID = 0
SERVER_PORT = 1234
THREAD_POOL_ON = True
PROPOSER = None
PROPOSED_VALUE = None
IS_PROPOSED = False
DECISIONS1 = {}
DECISIONS1_FLAGS = {}
DECISIONS2 = {}
DECISIONS2_FLAGS = {}
SERVERS = []
ACCEPTED_VALUE = None
IS_ACCEPTED1 = False
IS_ACCEPTED2 = False


def match(temp, tup):
    if len(temp) != len(tup):
        return False

    for i, j in zip(temp, tup):
        if i is None:
            continue
        elif i != j:
            return False

    return True


def enter_r(sock, p):
    QR.append((sock, p))
    if QR[0][1] == p:
        send_message(sock, {'resp': 'go'})
        inp(sock)


def exit_r(p):
    if QR[0][1] == p:
        unlock()


def unlock():
    QR.popleft()
    if len(QR) > 0:
        send_message(QR[0][0], {'resp': 'go'})
        inp(QR[0][0])


def out(t):
    if t not in RS:
        TS.add(t)

    try:
        RS.remove(t)
    except KeyError:
        pass


def rdp(sock, temp):
    temp_set = set()

    for t in TS:
        if match(temp, t):
            temp_set.add(t)

    send_message(sock, {'ts': temp_set})


def inp(sock):
    d = paxos(sock)
    if d is not None:
        if d not in TS:
            RS.add(d)

        TS.remove(d)


def paxos(sock):
    global IS_ACCEPTED2, ACCEPTED_VALUE
    while not IS_ACCEPTED2:
        pass

    send_message(sock, {'resp': ACCEPTED_VALUE})
    return ACCEPTED_VALUE


def propose(client_id, tup):
    global PROPOSED_VALUE, PROPOSER, IS_PROPOSED, QR
    if QR[0][1] == client_id:
        PROPOSER = client_id
        PROPOSED_VALUE = tup
        IS_PROPOSED = True
        make_decision(client_id)


def accept1(client_id, server_id, tup):
    global DECISIONS1, DECISIONS1_FLAGS, IS_PROPOSED, QR
    if IS_PROPOSED and [0][1] == client_id:
        DECISIONS1[server_id] = tup
        DECISIONS1_FLAGS[server_id] = True


def accept2(client_id, server_id, tup_dict):
    global DECISIONS2, DECISIONS2_FLAGS, IS_ACCEPTED1, QR
    if IS_ACCEPTED1 and QR[0][1] == client_id:
        DECISIONS2[server_id] = tup_dict
        DECISIONS2_FLAGS[server_id] = True


def make_decision(client_id):
    global SERVERS, DECISIONS1, DECISIONS2, SERVER_PORT, ACCEPTED_VALUE
    global IS_ACCEPTED1, IS_ACCEPTED2, DECISIONS1_FLAGS, DECISIONS2_FLAGS, TS
    if PROPOSED_VALUE in TS:
        DECISIONS1[SERVER_PORT] = PROPOSED_VALUE
    else:
        DECISIONS1[SERVER_PORT] = None
    DECISIONS1_FLAGS[SERVER_PORT] = True

    for i in SERVERS:
        if i != SERVER_PORT:
            sock = connect_to_server(i)
            send_message(sock, {'op': 'accept1', 'pid': client_id, 'sid': SERVER_PORT, 'tup': DECISIONS1[SERVER_PORT]})
            sock.close()

    while True:
        if False not in DECISIONS1_FLAGS.values():
            break
    IS_ACCEPTED1 = True

    DECISIONS2[SERVER_PORT] = DECISIONS1
    DECISIONS2_FLAGS[SERVER_PORT] = True

    for i in SERVERS:
        if i != SERVER_PORT:
            sock = connect_to_server(i)
            send_message(sock, {'op': 'accept2', 'pid': client_id,
                                'sid': SERVER_PORT, 'tup_dict': DECISIONS2[SERVER_PORT]})
            sock.close()

    while True:
        if False not in DECISIONS2.values():
            break

    accepted_values = {}
    for i in DECISIONS2.values():
        for j in i:
            if j in accepted_values.keys():
                accepted_values[j] += 1
            else:
                accepted_values[j] = 1

    n = len(SERVERS)**2 // 2 + 1
    for i in accepted_values.keys():
        if accepted_values[i] >= n:
            ACCEPTED_VALUE = accepted_values[i]
            IS_ACCEPTED2 = True
            break


def worker(client):
    global THREAD_POOL_ON
    req = get_message(client)

    if req['op'] == 'out':
        out(req['tup'])
    elif req['op'] == 'rd':
        rdp(client, req['temp'])
    elif req['op'] == 'enter':
        enter_r(client, req['pid'])
    elif req['op'] == 'exit':
        exit_r(req['pid'])
    elif req['op'] == 'propose':
        propose(req['pid'], req['tup'])
    elif req['op'] == 'accept1':
        accept1(req['pid'], req['sid'], req['tup'])
    elif req['op'] == 'accept2':
        accept2(req['pid'], req['sid'], req['tup_dict'])
    elif req['op'] == 'stop':
        logging.info('just got stop-message')
        THREAD_POOL_ON = False
    else:
        logging.info('Wrong request: ' + str(req) + '\n')


def read_from_ts_file(file_name):
    global TS
    with open(file_name, 'r') as f:
        data = json.load(f)
        for i in data:
            TS.add(tuple(i))
        # logging.info("TS: " + str(data))


""""""
parser = argparse.ArgumentParser()
parser.add_argument('id', type=int)
parser.add_argument('port', type=int)
parser.add_argument('TSFile', type=str)
parser.add_argument('quorum', nargs='+', type=int)
args = parser.parse_args()
SERVER_ID, SERVER_PORT, ts_file, SERVERS = args.id, args.port, args.TSFile, args.quorum


# SERVER_ID, SERVER_PORT, ts_file, SERVERS = 0, 1239, 'right.txt', [1239]

logging.basicConfig(filename=str(SERVER_ID) + 'log.txt', level=logging.DEBUG, format="%(asctime)s - %(message)s")
logging.info('Server ' + str(SERVER_ID) + ' on port ' + str(SERVER_PORT))
read_from_ts_file(ts_file)
logging.info('Red from file: ' + str(ts_file))
s = socket(AF_INET, SOCK_STREAM)
# s.settimeout(10)
s.bind(('', SERVER_PORT))
s.listen(1)

"""
with ThreadPoolExecutor(10) as pool:
    while THREAD_POOL_ON:
        try:
            client_s, client_addr = s.accept()
        except error:
            logging.error('socket.error in main while accepting')
            break
        pool.submit(worker, client_s)
"""

while THREAD_POOL_ON:
    try:
        client_s, client_addr = s.accept()
    except error:
        logging.error('socket.error in main while accepting')
        break
    worker(client_s)

s.close()

