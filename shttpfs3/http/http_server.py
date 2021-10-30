import json
import os
import socket
from typing import Union
import _thread

from shttpfs3.http.http_common import read_body, parse_http_request_preamble

#=====================
class Request:
    def __init__ (self, remote_addr: str, remote_port: int, uri: str, headers: dict, body: read_body):
        self.remote_addr = remote_addr
        self.remote_port = remote_port
        self.uri = uri
        self.headers = headers
        self.body    = body

    def get_json(self):
        return json.loads(self.body.read_all())

#=====================
class ServeFile:
    def __init__ (self, path: str):
        self.path = path

#=====================
class Responce:
    def __init__ (self, headers = None, body: Union[bytes, ServeFile] = b""):
        if headers is None: headers = {}
        self.headers = headers
        self.body    = body

#=============================================
def HTTPServer(host, port, connection_handler):
    def handle_connection(c, addr):
        try:
            while True:
                data = b""

                # read request preamble
                while True:
                    data += c.recv(1024)
                    if data == b'': raise Exception('Socket closed')
                    if b"\r\n\r\n" in data: break

                preamble, body_partial = data.split(b"\r\n\r\n", 1)


                # parse the header
                request = parse_http_request_preamble(preamble)

                if request['method'].lower() != 'post':
                    print('error parsing request')
                    break

                request_headers = {k.lower() : v for k,v in dict(request['headers']).items()}

                # handle the request
                print('Connection from:', addr[0], ':', addr[1],' ', request['path'])

                body_length = int(request_headers['content-length'])
                body_reader = read_body(c.recv, body_length, body_partial)
                rq = Request(addr[0], addr[1], request['path'], request_headers, body_reader)
                rsp: Responce = connection_handler(rq)
                body_reader.dump() # as we are using persistant connections, we need to read any
                                   # body from the socket

                # generate client responce
                responce_headers =  b"HTTP/1.1 200 OK\r\n"
                responce_headers += b"Connection: Keep-Alive\r\n"

                responce_content_length: int

                if isinstance(rsp.body, ServeFile):
                    responce_content_length = os.stat(rsp.body.path).st_size
                else:
                    responce_content_length = len(rsp.body)

                responce_headers += b"Content-Length: " + bytes(str(responce_content_length), encoding='utf8') + b'\r\n'

                for k, v in rsp.headers.items():
                    if isinstance(k, str): k=k.encode('utf8')
                    if isinstance(v, str): v=v.encode('utf8')
                    responce_headers += k + b':' + v + b'\r\n'

                responce_headers += b"\r\n"
                c.send(responce_headers)

                if isinstance(rsp.body, ServeFile):
                    c.sendfile(open(rsp.body.path, 'rb'), 0)
                else:
                    c.send(rsp.body)

        except:
            c.close()
            print('Connection handler thread crashed')

        c.close()

    #============
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # TODO add timeout
    s.bind((host, port))
    print("socket bound to port", port)

    # put the socket into listening mode
    s.listen(5)
    print("socket is listening")

    try:
        while True:
            c, addr = s.accept()

            # Start a new thread and return its identifier
            _thread.start_new_thread(handle_connection, (c, addr))
    except:
        s.close()
        raise

    s.close()
