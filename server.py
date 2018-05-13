from secondary_functions import *
import argparse
from concurrent.futures import ThreadPoolExecutor
from collections import deque
from collections import Counter
import logging
# from OpenSSL import SSL


TS = set()
RS = set()
QR = deque()
SERVER_ID = 0
SERVER_PORT = 0
THREAD_POOL_ON = True
DECISIONS1 = {}
AMOUNT_OF_DECISIONS1 = 0
DECISIONS2 = {}
AMOUNT_OF_DECISIONS2 = 0
SERVERS = []
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


def enter_r(sock, pid, temp):
    QR.append((sock, pid, temp))
    if QR[0][1] == pid:
        temp_set = rdp(None, temp)
        send_message(sock, {'resp': 'go', 'ts': temp_set})


def exit_r(p):
    if QR[0][1] == p:
        unlock()


def unlock():
    QR.popleft()
    if len(QR) > 0:
        temp_set = rdp(None, QR[0][2])
        send_message(QR[0][0], {'resp': 'go', 'ts': temp_set})


def out(t):
    if t not in RS:
        TS.add(t)
        logging.info('Add to TS: ' + str(t))

    try:
        RS.remove(t)
    except KeyError:
        pass
    else:
        logging.info('Remove from RS: ' + str(t))


def rdp(sock, temp):
    temp_set = set()

    for t in TS:
        if match(temp, t):
            temp_set.add(t)

    logging.info('Temp_set for ' + str(temp) + ' : ' + str(temp_set))
    if sock is not None:
        send_message(sock, {'ts': temp_set})
    else:
        return temp_set


def inp(sock, p, tup):
    if QR[0][1] == p:
        d = paxos(sock, tup)
        if d is not None:
            if d not in TS:
                RS.add(d)
                logging.info('Add to RS: ' + str(d))

            TS.remove(d)


def paxos(sock, tup):
    global IS_ACCEPTED1, IS_ACCEPTED2, SERVERS, DECISIONS1, SERVER_PORT,\
        AMOUNT_OF_DECISIONS1, DECISIONS2, AMOUNT_OF_DECISIONS2
    if tup in TS:
        accepted_value = tup
    else:
        accepted_value = None

    logging.info("accepted value: " + str(accepted_value))

    AMOUNT_OF_DECISIONS1 = 0
    DECISIONS1 = {i: [False, None] for i in SERVERS}  # port: (ans_flag, tup)
    connected = 0
    for i in SERVERS:
        if i == SERVER_PORT:
            DECISIONS1[i][0] = True
            DECISIONS1[i][1] = accepted_value
        else:
            serv_sock = connect_to_server(i)
            if serv_sock is not None:
                connected += 1
                send_message(serv_sock,
                             {'op': 'accept1', 'pid': QR[0][1], 'port': SERVER_PORT, 'tup': accepted_value})
                logging.info("Send accepted value on port " + str(i))
                serv_sock.close()
            else:
                DECISIONS1.pop(i)

    i = 500
    while AMOUNT_OF_DECISIONS1 < connected and i > 0:
        i -= 1

    IS_ACCEPTED1 = True
    logging.info("IS_ACCEPTED1 = True")
    # logging.info("DECISIONS1: " + str(DECISIONS1))
    tup_dict = {}  # port: tup // ACCEPT1 results
    for i in DECISIONS1.keys():
        tup_dict[i] = DECISIONS1[i][1]

    AMOUNT_OF_DECISIONS2 = 0
    DECISIONS2 = {i: [False, None] for i in SERVERS}  # port: (ans_flag, tup_dict)
    connected = 0
    for i in DECISIONS1.keys():
        if i == SERVER_PORT:
            DECISIONS2[i][0] = True
            DECISIONS2[i][1] = tup_dict
        else:
            serv_sock = connect_to_server(i)
            if serv_sock is not None:
                connected += 1
                send_message(serv_sock,
                             {'op': 'accept2', 'pid': QR[0][1], 'port': SERVER_PORT, 'tup_dict': tup_dict})
                logging.info("Send accepted values on port " + str(i))
                serv_sock.close()
            else:
                DECISIONS2.pop(i)

    i = 500
    while AMOUNT_OF_DECISIONS2 < connected and i > 0:
        i -= 1

    IS_ACCEPTED2 = True
    logging.info("IS_ACCEPTED2 = True")
    # logging.info("DECISIONS2: " + str(DECISIONS2))
    tup_list1 = []
    try:
        for i in tup_dict.keys():
            if i == SERVER_PORT:
                tup_list1.append(accepted_value)
                continue
            tup_list2 = []
            for j in DECISIONS2.keys():
                if j == i or not DECISIONS2[j][0]:
                    continue
                tup_list2.append(DECISIONS2[j][1][i])
            # logging.info("tup_list2: " + str(tup_list2))
            c = Counter(tup_list2)
            mc = c.most_common(1)[0]
            # logging.info("mc: " + str(mc))
            if mc[1] > len(tup_list2) // 2:
                tup_list1.append(mc[0])
            else:
                tup_list1.append(None)
    except Exception as e:
        logging.info("Error" + str(e))

    # logging.info("tup_list1: " + str(tup_list1))
    c = Counter(tup_list1)
    accepted_value = c.most_common(1)[0][0]
    send_message(sock, {'resp': accepted_value})
    logging.info("Send twice accepted value: " + str(accepted_value))
    return accepted_value


def accept1(p, server_port, tup):
    global DECISIONS1, QR, AMOUNT_OF_DECISIONS1
    if QR[0][1] == p and not DECISIONS1[server_port][0]:
        DECISIONS1[server_port][1] = tup
        DECISIONS1[server_port][0] = True
        AMOUNT_OF_DECISIONS1 += 1
        logging.info("Get accepted value from port " + str(server_port))


def accept2(p, server_port, tup_dict):
    global DECISIONS2, AMOUNT_OF_DECISIONS2, IS_ACCEPTED1, QR
    if QR[0][1] == p and not DECISIONS2[server_port][0]:
        DECISIONS2[server_port][1] = tup_dict
        DECISIONS2[server_port][0] = True
        AMOUNT_OF_DECISIONS2 += 1
        logging.info("Get accepted values from port " + str(server_port))


def worker(client):
    global THREAD_POOL_ON
    req = get_message(client)

    if req['op'] == 'out':
        out(req['tup'])
    elif req['op'] == 'rd':
        rdp(client, req['temp'])
    elif req['op'] == 'inp':
        inp(client, req['pid'], req['tup'])
    elif req['op'] == 'enter':
        enter_r(client, req['pid'], req['temp'])
    elif req['op'] == 'exit':
        exit_r(req['pid'])
    elif req['op'] == 'accept1':
        accept1(req['pid'], req['port'], req['tup'])
    elif req['op'] == 'accept2':
        accept2(req['pid'], req['port'], req['tup_dict'])
    elif req['op'] == 'stop':
        logging.info('Stop-message received')
        THREAD_POOL_ON = False
    else:
        logging.info('Wrong request: ' + str(req))

    client.close()


def read_from_ts_file(file_name):
    global TS
    with open(file_name, 'r') as f:
        data = json.load(f)
        for i in data:
            TS.add(tuple(i))
        # logging.info("TS: " + str(data))


parser = argparse.ArgumentParser()
parser.add_argument('id', type=int)
parser.add_argument('port', type=int)
parser.add_argument('TSFile', type=str)
parser.add_argument('quorum', nargs='+', type=int)
args = parser.parse_args()
SERVER_ID, SERVER_PORT, ts_file, SERVERS = args.id, args.port, args.TSFile, args.quorum
"""

SERVER_ID, SERVER_PORT, ts_file, SERVERS = 0, 1235, 'right.txt', [1234, 1235]
"""

logging.basicConfig(filename=str(SERVER_ID) + 'log.txt', level=logging.DEBUG, format="%(asctime)s - %(message)s")
logging.info('Server ' + str(SERVER_ID) + ' on port ' + str(SERVER_PORT))
read_from_ts_file(ts_file)
logging.info('Red from file: ' + str(ts_file))
s = socket(AF_INET, SOCK_STREAM)
s.bind(('', SERVER_PORT))
s.listen(1)


with ThreadPoolExecutor(20) as pool:
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
"""
s.close()
