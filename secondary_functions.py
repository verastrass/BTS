from socket import socket, AF_INET, SOCK_STREAM, error
import _pickle as pickle
import json


def get_message(sock):
    # sock.settimeout(15)
    msg = b""
    tmp = b""

    while True:
        try:
            tmp = sock.recv(1024)
        except error:
            break
        if len(tmp) < 1024:
            break
        msg += tmp
    msg += tmp
    return pickle.loads(msg)


def send_message(sock, msg):
    sock.send(pickle.dumps(msg))


def connect_to_server(port):
    sock = socket(AF_INET, SOCK_STREAM)
    i = 100

    while i:
        try:
            sock.connect(('localhost', port))
        except ConnectionRefusedError:
            i -= 1
        else:
            return sock

    return None


def create_ts_file(file_name, init_num, amount=500):
    with open(file_name, 'w') as f:
        list_of_tuples = list()
        for i in range(init_num * amount, (init_num + 1) * amount):
            list_of_tuples.append(tuple(j for j in range(i, i + 100)))
        json.dump(list_of_tuples, f)
