import os, json, urllib.parse
from typing import Dict
from bversion.http.http_client import HTTPClient

class client_http_request:
############################################################################################
    def __init__(self, server_base_url: str):
        "Configure the servers base URL"

        res = urllib.parse.urlparse(server_base_url)
        scheme               = res.scheme.lower()
        server_base_url      = res.hostname
        self.server_base_url = server_base_url
        port                 = res.port

        if scheme not in ['http', 'https']: raise SystemExit('unknown protocol: ' + scheme)

        if port is None: port = 443 if scheme == 'https' else 80

        self.c = HTTPClient()
        self.c.connect( server_base_url,
                        port = port,
                        tls  = (scheme == 'https'))

############################################################################################
    def begin(self, url: str, body_length: int, add_headers: Dict[str, str], content_type: str):
        headers = {
            'Accept-Encoding': 'identity',
            'Host'           : self.server_base_url,
            'Content-Type'   : content_type,
            'Content-length' : str(body_length),
            'User-Agent'     : 'BVersion' }

        for k, v in add_headers.items(): headers[k] = v

        # ==
        self.c.send_headers('/' + url, headers)
        return self.c

############################################################################################
    def request(self, url, headers, data = None, gen = False):
        jsn = json.dumps(data) if data is not None else '{}'

        conn = self.begin(url, len(jsn), headers, content_type = 'application/json')
        conn.send(jsn.encode('utf8'))
        parsed_preamble, body = conn.read_responce()

        if gen is False:
            return body.read_all(), parsed_preamble['headers']

        else:
            def writer(path):
                with open(path, 'wb') as f:
                    while True:
                        chunk = body.read(1000 * 1000)
                        if chunk is None: break
                        f.write(chunk)

            return writer, parsed_preamble['headers']

############################################################################################
    def send_file(self, url, headers, file_path):
        size = os.stat(file_path).st_size

        conn = self.begin(url, size, headers, content_type = 'application/octet-stream')

        with open(file_path, 'rb') as f:
            while True:
                chunk = f.read(1000 * 1000)
                if chunk == b'': break
                conn.send(chunk)

        parsed_preamble, body = conn.read_responce()
        return body.read_all(), parsed_preamble['headers']
        
