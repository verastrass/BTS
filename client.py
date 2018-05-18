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
            logging.info('OUT: ' + str(tup))
        else:
            logging.info('OUT_OP: cannot connect to server')

    def rd_op(self, temp):
        sock = connect_to_server(self.port)
        if sock is not None:
            send_message(sock, {'op': 'rdp', 'temp': temp})
            # logging.info('send: ' + str(temp))
            resp = get_message(sock)
            # logging.info('get: ' + str(resp))
            sock.close()
            logging.info('RD: ' + str(resp['resp']))
            if resp is None:
                return resp
            else:
                return resp['resp']
        else:
            logging.info('RD_OP: cannot connect to server')
            return None

    def in_op(self, temp):
        sock = connect_to_server(self.port)
        if sock is not None:
            send_message(sock, {'op': 'inp', 'temp': temp})
            resp = get_message(sock)
            sock.close()
            logging.info('In: ' + str(resp['resp']))
            if resp is None:
                return resp
            else:
                return resp['resp']
        else:
            logging.info('IN_OP: cannot connect to server')
            return None

    def stop_op(self):
        sock = connect_to_server(self.port)
        if sock is not None:
            send_message(sock, {'op': 'stop'})
            resp = get_message(sock)
            sock.close()
            logging.info('STOP_OP: ' + str(resp['resp']))
            if resp is None:
                return resp
            else:
                return resp['resp']
        else:
            logging.info('STOP_OP: cannot connect to server')
            return None
