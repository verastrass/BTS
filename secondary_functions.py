from socket import socket, AF_INET, SOCK_STREAM, error
import pickle
import json


def get_message(sock):
    # sock.settimeout(600)
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

    if len(msg) > 0:
        return pickle.loads(msg)
    return None


def send_message(sock, msg):
    return sock.send(pickle.dumps(msg))


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


def create_ts_file(file_name, init_num, amount=500, length=100):
    with open(file_name, 'w') as f:
        list_of_tuples = list()
        for i in range(init_num * amount, (init_num + 1) * amount):
            list_of_tuples.append(tuple(j for j in range(i, i + length)))
        json.dump(list_of_tuples, f)
