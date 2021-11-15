import socket, ssl
from typing import Dict

from bversion.http.http_common import read_body, generate_headers, parse_http_responce_preamble

#=====================================================================
class HTTPClient:
    def __init__(self):
        self.s = None

    def connect(self, host: str, port: int, tls: bool = False):
        self.s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)

        if tls: self.s = ssl.wrap_socket(self.s, ssl_version=ssl.PROTOCOL_TLS)
        self.s.connect((host,port))

    def send(self, data: bytes):
        self.s.send(data)

    def send_headers(self, uri: str, headers: Dict[str, str]):
        msg = b"POST " + uri.encode('utf8') + b" HTTP/1.1\r\n"
        self.send(
            msg +
            generate_headers(headers) +
            b"\r\n"
        )

    def read_responce(self):
        parsed_preamble, body_partial = self.read_headers()
        body = read_body(self.s.recv, int(parsed_preamble['headers']['content-length']), body_partial)
        return parsed_preamble, body

    def read_headers(self):
        data = b""

        # read request preamble
        while True:
            data += self.s.recv(1024)
            if b"\r\n\r\n" in data: break

        preamble, body_partial = data.split(b"\r\n\r\n", 1)
        parsed_preamble = parse_http_responce_preamble(preamble)
        return parsed_preamble, body_partial

    def close(self):
        self.s.close()
