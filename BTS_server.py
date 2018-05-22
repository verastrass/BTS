from secondary_functions import *
import argparse
from concurrent.futures import ThreadPoolExecutor
from collections import deque, Counter
import logging


TS = set()
RS = set()
QR = {}         # temp: deque(sock, pid, temp, decisions1, AMOUNT_OF_DECISIONS1, decisions2, AMOUNT_OF_DECISIONS2)
SERVER_ID = 0
SERVER_PORT = 0
THREAD_POOL_ON = True
SERVERS = []

"""
DECISIONS1 = {}
AMOUNT_OF_DECISIONS1 = 0
DECISIONS2 = {}
AMOUNT_OF_DECISIONS2 = 0
"""



def match(temp, tup):
    if len(temp) != len(tup):
        return False

    for i, j in zip(temp, tup):
        if i is None:
            continue
        elif i != j:
            return False

    return True


def enter_r(sock, pid, temp):
    global QR
    added = False

    for i in QR.keys():
        if match(i, temp) or match(temp, i):
            QR[i].append([sock, pid, temp, {}, 0, {}, 0])
            added = True
            break

    if not added:
        QR[temp] = deque()
        QR[temp].append([sock, pid, temp, {}, 0, {}, 0])

    if QR[temp][0][1] == pid:
        logging.info('ENTER_R: ' + str(pid) + ' enter cs')
        temp_set = rdp(None, temp)
        send_message(sock, {'resp': 'go', 'ts': temp_set})


def exit_r(temp, pid=None):
    global QR
    if pid is None or QR[temp][0][1] == pid:
        logging.info('EXIT_R: ' + str(QR[temp][0][1]) + ' exit cs')
        QR[temp].popleft()
        if len(QR[temp]) > 0:
            temp_set = rdp(None, QR[temp][0][2])
            send_message(QR[temp][0][0], {'resp': 'go', 'ts': temp_set})


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

    logging.info('RDP: temp_set for ' + str(temp) + ' : ' + str(temp_set))
    if sock is not None:
        send_message(sock, {'ts': temp_set})
    else:
        return temp_set


def inp(sock, pid, temp, tup):
    global TS, RS, QR
    if QR[temp][0][1] == pid:
        d = paxos(sock, temp, tup)
        if d is not None:
            if d not in TS:
                RS.add(d)
                logging.info('INP ' + str(pid) + ': add ' + str(d) + ' to RS')

            TS.remove(d)
    exit_r(temp)


def paxos(sock, temp, tup):
    global SERVERS, QR, SERVER_PORT
    if tup in TS:
        accepted_value = tup
    else:
        accepted_value = None

    logging.info('PAXOS ' + str(QR[temp][0][1]) + ': value ' + str(accepted_value) + ' accepted')

    # DECISIONS1
    QR[temp][0][3] = {i: [False, None] for i in SERVERS}  # port: (ans_flag, tup)

    connected = 0
    for i in SERVERS:
        if i == SERVER_PORT:
            QR[temp][0][3][i][0] = True
            QR[temp][0][3][i][1] = accepted_value
        else:
            serv_sock = connect_to_server(i)
            if serv_sock is not None:
                connected += 1
                send_message(serv_sock,
                             {'op': 'accept1', 'pid': QR[temp][0][1], 'temp': temp, 'port': SERVER_PORT, 'tup': accepted_value})
                logging.info('PAXOS '+str(QR[temp][0][1])+': send accepted value '+str(accepted_value)+' to '+str(i))
                serv_sock.close()
            else:
                QR[temp][0][3].pop(i)

    i = 500
    while QR[temp][0][4] < connected and i > 0:
        i -= 1

    logging.info('PAXOS ' + str(QR[temp][0][1]) + ': is accepted 1')
    # logging.info("QR[temp][0][3]: " + str(QR[temp][0][3]))
    tup_dict = {}  # port: tup // ACCEPT1 results
    for i in QR[temp][0][3].keys():
        tup_dict[i] = QR[temp][0][3][i][1]

    # DECISIONS2
    QR[temp][0][5] = {i: [False, None] for i in SERVERS}  # port: (ans_flag, tup_dict)
    connected = 0
    for i in QR[temp][0][3].keys():
        if i == SERVER_PORT:
            QR[temp][0][5][i][0] = True
            QR[temp][0][5][i][1] = tup_dict
        else:
            serv_sock = connect_to_server(i)
            if serv_sock is not None:
                connected += 1
                send_message(serv_sock,
                             {'op': 'accept2', 'pid': QR[temp][0][1], 'temp': temp, 'port': SERVER_PORT, 'tup_dict': tup_dict})
                logging.info('PAXOS '+str(QR[temp][0][1])+': send accepted values '+str(tup_dict)+' to '+str(i))
                serv_sock.close()
            else:
                QR[temp][0][5].pop(i)

    i = 500
    while QR[temp][0][6] < connected and i > 0:
        i -= 1

    logging.info('PAXOS ' + str(QR[temp][0][1]) + ': is accepted 2')
    # logging.info("QR[temp][0][5]: " + str(QR[temp][0][5]))
    tup_list1 = []
    try:
        for i in tup_dict.keys():
            if i == SERVER_PORT:
                tup_list1.append(accepted_value)
                continue
            tup_list2 = []
            for j in QR[temp][0][5].keys():
                if j == i or not QR[temp][0][5][j][0]:
                    continue
                tup_list2.append(QR[temp][0][5][j][1][i])
            # logging.info("tup_list2: " + str(tup_list2))
            c = Counter(tup_list2)
            mc = c.most_common(1)[0]
            # logging.info("mc: " + str(mc))
            if mc[1] > len(tup_list2) // 2:
                tup_list1.append(mc[0])
            else:
                tup_list1.append(None)
    except Exception as e:
        logging.info('PAXOS ' + str(QR[temp][0][1]) + ': error ' + str(e))

    # logging.info("tup_list1: " + str(tup_list1))
    c = Counter(tup_list1)
    accepted_value = c.most_common(1)[0][0]
    send_message(sock, {'resp': accepted_value})
    logging.info('PAXOS ' + str(QR[temp][0][1]) + ': send twice accepted value ' + str(accepted_value))
    return accepted_value


def accept1(p, temp, server_port, tup):
    global QR
    if QR[temp][0][1] == p and not QR[temp][0][3][server_port][0]:
        QR[temp][0][3][server_port][1] = tup
        QR[temp][0][3][server_port][0] = True
        QR[temp][0][4] += 1
        logging.info('ACCEPT1 '+str(QR[temp][0][1])+': get accepted value '+str(tup)+' from port '+str(server_port))


def accept2(p, temp, server_port, tup_dict):
    global QR
    if QR[temp][0][1] == p and not QR[temp][0][5][server_port][0]:
        QR[temp][0][5][server_port][1] = tup_dict
        QR[temp][0][5][server_port][0] = True
        QR[temp][0][6] += 1
        logging.info('ACCEPT2 '+str(QR[temp][0][1])+': get accepted values '+str(tup_dict)+' from port '+str(server_port))


def worker(client):
    global THREAD_POOL_ON
    req = get_message(client)
    if req is None:
        return

    if req['op'] == 'out':
        out(req['tup'])
    elif req['op'] == 'rd':
        rdp(client, req['temp'])
    elif req['op'] == 'in':
        try:
            inp(client, req['pid'], req['temp'], req['tup'])
        except Exception as e:
            logging.info('INP: error ' + str(e))
    elif req['op'] == 'enter':
        enter_r(client, req['pid'], req['temp'])
    elif req['op'] == 'exit':
        exit_r(req['temp'], req['pid'])
    elif req['op'] == 'accept1':
        accept1(req['pid'], req['temp'], req['port'], req['tup'])
    elif req['op'] == 'accept2':
        accept2(req['pid'], req['temp'], req['port'], req['tup_dict'])
    elif req['op'] == 'stop':
        logging.info('WORKER: stop-message received')
        THREAD_POOL_ON = False
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

SERVER_ID, SERVER_PORT, ts_file, SERVERS = 0, 1234, 'right.txt', [1234]
"""

logging.basicConfig(filename=str(SERVER_ID) + 'log.txt', level=logging.DEBUG, format="%(asctime)s - %(message)s")
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
        except error:
            pass
            # logging.error('MAIN: socket.error while accepting')
            # break
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

s.close()
logging.info('RUN: stop working')
