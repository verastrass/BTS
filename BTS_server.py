from secondary_functions import *
import argparse
from concurrent.futures import ThreadPoolExecutor
from collections import deque, Counter
import logging


TS = set()
RS = set()
QR = {}         # temp: deque(sock, pid, temp, decisions1, AMOUNT_OF_DECISIONS1, decisions2, AMOUNT_OF_DECISIONS2)
TYPES = {}
SERVER_ID = 0
SERVER_PORT = 0
THREAD_POOL_ON = True
SERVERS = []



def match(temp, tup):
    if len(temp) != len(tup):
        return False

    for i, j in zip(temp, tup):
        if i is None:
            continue
        elif i != j:
            return False

    return True


def equal_types(t1, t2):
    if len(t1) != len(t2):
        return False

    for i, j in zip(t1, t2):
        if i is 'NoneType' or j is 'NoneType':
            continue
        elif i != j:
            return False

    return True


def get_type(tup):
    t = []

    for i in tup:
        t.append(type(i))

    return tuple(t)


def enter_r(sock, pid, temp):
    global QR, TYPES
    added = False
    temp_type = get_type(temp)

    for i in QR.keys():
        if equal_types(i, temp_type):
            QR[i].append([sock, pid, temp, {i: [False, None] for i in SERVERS}, 0, {i: [False, None] for i in SERVERS}, 0])
            added = True
            TYPES[temp] = i
            break

    if not added:
        QR[temp_type] = deque()
        QR[temp_type].append([sock, pid, temp, {i: [False, None] for i in SERVERS}, 0, {i: [False, None] for i in SERVERS}, 0])
        TYPES[temp] = temp_type

    if QR[TYPES[temp]][0][1] == pid:
        temp_set = rdp(None, temp)
        send_message(sock, {'resp': 'go', 'ts': temp_set})
        logging.info('ENTER_R: ' + str(pid) + ' enter cs')


def exit_r(temp, pid=None):
    global QR, TYPES
    if pid is None or QR[TYPES[temp]][0][1] == pid:
        logging.info('EXIT_R: ' + str(QR[TYPES[temp]][0][1]) + ' exit cs')
        QR[TYPES[temp]].popleft()
        if len(QR[TYPES[temp]]) > 0:
            temp_set = rdp(None, QR[TYPES[temp]][0][2])
            send_message(QR[TYPES[temp]][0][0], {'resp': 'go', 'ts': temp_set})
            logging.info('EXIT_R: ' + str(QR[TYPES[temp]][0][1]) + ' enter cs')


def out(t):
    global TS, RS
    if t not in RS:
        TS.add(t)
        logging.info('OUT: add ' + str(t) + ' to TS')

    try:
        RS.remove(t)
    except KeyError:
        pass
    else:
        logging.info('OUT: remove ' + str(t) + ' from RS')


def rdp(sock, temp):
    global TS
    temp_set = set()

    for t in TS:
        if match(temp, t):
            temp_set.add(t)

    logging.info('RDP: temp ' + str(temp) + ', set ' + str(temp_set))
    if sock is not None:
        send_message(sock, {'ts': temp_set})
    else:
        return temp_set


def inp(sock, pid, temp, tup):
    global TS, RS, QR, TYPES
    logging.info('INP ' + str(pid) + ': propose ' + str(tup))
    if QR[TYPES[temp]][0][1] == pid:
        d = paxos(sock, temp, tup)
        if d is not None:
            if d not in TS:
                RS.add(d)
                logging.info('INP ' + str(pid) + ': add ' + str(d) + ' to RS')

            try:
                TS.remove(d)
            except KeyError:
                pass
            else:
                logging.info('INP: remove ' + str(d) + ' from TS')

        exit_r(temp)


def paxos(sock, temp, tup):
    global SERVERS, QR, SERVER_PORT, TYPES
    temp_type = TYPES[temp]
    logging.info('PAXOS: ' + str(QR))

    if (tup in TS) and (tup not in RS):
        accepted_value = tup
    else:
        accepted_value = None

    logging.info('PAXOS ' + str(QR[temp_type][0][1]) + ': value ' + str(accepted_value) + ' accepted')

    # DECISIONS1
    connected = 0
    for i in SERVERS:
        if i == SERVER_PORT:
            QR[temp_type][0][3][i][0] = True
            QR[temp_type][0][3][i][1] = accepted_value
        else:
            serv_sock = connect_to_server(i)
            if serv_sock is not None:
                connected += 1
                size = send_message(serv_sock, {'op': 'accept1', 'pid': QR[temp_type][0][1], 'temp': temp,
                                         'port': SERVER_PORT, 'tup': accepted_value})
                logging.info('PAXOS: send ' + str(size) + ' to ' + str(i))
                logging.info('PAXOS '+str(QR[temp_type][0][1])+': send accepted value '+str(accepted_value)+' to '+str(i))
                serv_sock.close()
            else:
                QR[temp_type][0][3].pop(i)

    i = 10000 * len(SERVERS)
    while QR[temp_type][0][4] < connected and i > 0:
        i -= 1
        pass

    logging.info('PAXOS ' + str(QR[temp_type][0][1]) + ': is accepted 1')
    tup_dict = {}  # port: tup // ACCEPT1 results
    for i in QR[temp_type][0][3].keys():
        tup_dict[i] = QR[temp_type][0][3][i][1]

    # DECISIONS2
    connected = 0
    for i in QR[temp_type][0][3].keys():
        if i == SERVER_PORT:
            QR[temp_type][0][5][i][0] = True
            QR[temp_type][0][5][i][1] = tup_dict
        else:
            serv_sock = connect_to_server(i)
            if serv_sock is not None:
                connected += 1
                send_message(serv_sock,
                             {'op': 'accept2', 'pid': QR[temp_type][0][1], 'temp': temp, 'port': SERVER_PORT, 'tup_dict': tup_dict})
                logging.info('PAXOS '+str(QR[temp_type][0][1])+': send accepted values '+str(tup_dict)+' to '+str(i))
                serv_sock.close()
            else:
                QR[temp_type][0][5].pop(i)

    i = 10000 * len(SERVERS)
    while QR[temp_type][0][6] < connected and i > 0:
        i -= 1
        pass

    logging.info('PAXOS ' + str(QR[temp_type][0][1]) + ': is accepted 2')
    tup_list1 = []
    try:
        for i in tup_dict.keys():
            if i == SERVER_PORT:
                tup_list1.append(accepted_value)
                continue
            tup_list2 = []
            for j in QR[temp_type][0][5].keys():
                if j == i or not QR[temp_type][0][5][j][0]:
                    continue
                tup_list2.append(QR[temp_type][0][5][j][1][i])
            c = Counter(tup_list2)
            mc = c.most_common(1)[0]
            if mc[1] > len(tup_list2) // 2:
                tup_list1.append(mc[0])
            else:
                tup_list1.append(None)
    except Exception as e:
        logging.info('PAXOS ' + str(QR[temp_type][0][1]) + ': error ' + str(e))

    c = Counter(tup_list1)
    accepted_value = c.most_common(1)[0][0]
    send_message(sock, {'resp': accepted_value})
    logging.info('PAXOS ' + str(QR[temp_type][0][1]) + ': send twice accepted value ' + str(accepted_value))
    return accepted_value


def accept1(pid, temp, server_port, tup):
    global QR, TYPES
    temp_type = TYPES[temp]
    try:
        if QR[temp_type][0][1] == pid and not QR[temp_type][0][3][server_port][0]:
            QR[temp_type][0][3][server_port][1] = tup
            QR[temp_type][0][3][server_port][0] = True
            QR[temp_type][0][4] += 1
            logging.info('ACCEPT1 '+str(pid)+': get accepted value '+str(tup)+' from port '+str(server_port))
    except Exception as e:
        logging.info('ACCEPT1: error ' + str(e))


def accept2(pid, temp, server_port, tup_dict):
    global QR, TYPES
    temp_type = TYPES[temp]
    try:
        if QR[temp_type][0][1] == pid and not QR[temp_type][0][5][server_port][0]:
            QR[temp_type][0][5][server_port][1] = tup_dict
            QR[temp_type][0][5][server_port][0] = True
            QR[temp_type][0][6] += 1
            logging.info('ACCEPT2 '+str(pid)+': get accepted values '+str(tup_dict)+' from port '+str(server_port))
    except Exception as e:
        logging.info('ACCEPT2: error ' + str(e))


def worker(client):
    global THREAD_POOL_ON
    req = get_message(client)
    # logging.info('WORKER: message ' + str(req) + ' received')
    if req is None:
        client.close()
        return

    if req['op'] == 'out':
        out(req['tup'])
    elif req['op'] == 'rd':
        rdp(client, req['temp'])
    elif req['op'] == 'in':
        inp(client, req['pid'], req['temp'], req['tup'])
    elif req['op'] == 'enter':
        enter_r(client, req['pid'], req['temp'])
        return
    elif req['op'] == 'exit':
        exit_r(req['temp'], req['pid'])
    elif req['op'] == 'accept1':
        accept1(req['pid'], req['temp'], req['port'], req['tup'])
    elif req['op'] == 'accept2':
        accept2(req['pid'], req['temp'], req['port'], req['tup_dict'])
    elif req['op'] == 'stop':
        logging.info('WORKER: stop-message received')
        THREAD_POOL_ON = False
        return
    else:
        logging.info('WORKER: wrong request ' + str(req))

    client.close()


def read_from_ts_file(file_name):
    global TS
    with open(file_name, 'r') as f:
        data = json.load(f)
        for i in data:
            TS.add(tuple(i))


parser = argparse.ArgumentParser()
parser.add_argument('id', type=int)
parser.add_argument('port', type=int)
parser.add_argument('TSFile', type=str)
parser.add_argument('quorum', nargs='+', type=int)
args = parser.parse_args()
SERVER_ID, SERVER_PORT, ts_file, SERVERS = args.id, args.port, args.TSFile, args.quorum
"""

SERVER_ID, SERVER_PORT, ts_file, SERVERS = 0, 1234, 'right.txt', [1234, 1235, 1236]
"""

logging.basicConfig(filename=str(SERVER_ID) + 'log.txt', level=logging.INFO, format="%(asctime)s - %(message)s")
logging.info('Server ' + str(SERVER_ID) + ' on port ' + str(SERVER_PORT))
read_from_ts_file(ts_file)
logging.info('Read from file: ' + str(ts_file))
s = socket(AF_INET, SOCK_STREAM)
s.bind(('', SERVER_PORT))
s.listen(1)


with ThreadPoolExecutor(10) as pool:
    while THREAD_POOL_ON:
        try:
            s.settimeout(30)
            client_s, client_addr = s.accept()
            # logging.info('MAIN: accept')
        except error:
            # logging.error('MAIN: socket.error while accepting')
            pass
        else:
            pool.submit(worker, client_s)
"""

while THREAD_POOL_ON:
    try:
        client_s, client_addr = s.accept()
    except error:
        logging.error('MAIN: socket.error while accepting')
    break
else:
    worker(client_s)
"""

logging.info('RUN: stop working')
send_message(client_s, {'resp': 'ok'})
client_s.close()
s.close()

