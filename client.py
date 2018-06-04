import logging
from secondary_functions import get_message, send_message, connect_to_server


class Client(object):

    def __init__(self, port):
        self.port = port
        logging.basicConfig(filename='client_log.txt', level=logging.INFO, format="%(asctime)s - %(message)s")

    def __op(self, op_type, tup=None):
        sock = connect_to_server(self.port)
        if sock is not None:
            if op_type == 'out':
                req = {'op': op_type, 'tup': tup}
            elif op_type in ['rd', 'in']:
                req = {'op': op_type, 'temp': tup}
            elif op_type == 'stop':
                req = {'op': op_type}
            send_message(sock, req)
            if op_type in ['rd', 'in', 'stop']:
                resp = get_message(sock)
                sock.close()
                if resp is None:
                    return resp
                return resp['resp']
            else:
                sock.close()
        else:
            logging.info('__OP (' + str(op_type) + '): cannot connect to server')
            return None

    def out_op(self, tup):
        self.__op('out', tup)
        logging.info('OUT: ' + str(tup))

    def rd_op(self, temp):
        resp = self.__op('rd', temp)
        logging.info('RD: ' + str(resp))
        return resp

    def in_op(self, temp):
        resp = self.__op('in', temp)
        logging.info('IN: ' + str(resp))
        return resp

    def stop_op(self):
        resp = self.__op('stop')
        logging.info('STOP: ' + str(resp))
        return resp
