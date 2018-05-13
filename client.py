import logging
from secondary_functions import get_message, send_message, connect_to_server


class Client(object):

    def __init__(self, port):
        self.port = port
        logging.basicConfig(filename='client_log.txt', level=logging.DEBUG, format="%(asctime)s - %(message)s")

    def out_op(self, tup):
        sock = connect_to_server(self.port)
        if sock is not None:
            send_message(sock, {'op': 'out', 'tup': tup})
            sock.close()
        else:
            logging.info('Cannot connect to server in out_op')

    def rd_op(self, temp):
        sock = connect_to_server(self.port)
        if sock is not None:
            send_message(sock, {'op': 'rdp', 'temp': temp})
            logging.info('send: ' + str(temp))
            resp = get_message(sock)
            logging.info('get: ' + str(resp))
            sock.close()
            return resp['resp']
        else:
            logging.info('Cannot connect to server in rd_op')
            return None

    def in_op(self, temp):
        sock = connect_to_server(self.port)
        if sock is not None:
            send_message(sock, {'op': 'inp', 'temp': temp})
            resp = get_message(sock)
            sock.close()
            return resp['resp']
        else:
            logging.info('Cannot connect to server in in_op')
            return None

    def stop_op(self):
        sock = connect_to_server(self.port)
        if sock is not None:
            send_message(sock, {'op': 'stop'})
            resp = get_message(sock)
            sock.close()
            return resp['resp']
        else:
            logging.info('Cannot connect to server in stop_op')
            return None
