from typing import List, Dict, Union
from typing_extensions import TypedDict

#=====================================================================
def generate_headers(headers: Dict[str, str]) -> bytes:
    res: bytes = b''
    for k,v in headers.items():
        k2 = k.encode('utf8') if isinstance(k, str) else k
        v2 = v.encode('utf8') if isinstance(v, str) else v

        res += k2 + b': ' + v2 + b'\r\n'
    return res

#=====================================================================
def parse_headers(headers_raw: List[bytes]) -> Dict[str, str]:
    headers = {}
    for i in headers_raw:
        split_header = i.split(b':', 1)
        headers[split_header[0].strip().decode('utf8').lower()] = split_header[1].strip().decode('utf8')

    return headers

#=====================================================================
class httpResponcePreamble(TypedDict):
    protocol: bytes
    status:   List[bytes]
    headers:  Dict[str, str]

#=====================================================================
def parse_http_responce_preamble(preamble: bytes) -> httpResponcePreamble:
    split = preamble.split(b"\r\n")
    statusline = split[0]
    headers_raw = split[1:]

    split_status = statusline.split(b' ')
    if len(split_status) != 3: raise Exception('Badly formatted status line')

    return {'protocol' : split_status[0],
            'status'   : split_status[1:2],
            'headers'  : parse_headers(headers_raw)}

#=====================================================================
class httpRequestPreamble(TypedDict):
    method:   str
    path:     str
    headers:  Dict[str, str]

#=====================================================================
def parse_http_request_preamble(preamble: bytes) -> httpRequestPreamble:
    split = preamble.split(b"\r\n")
    statusline = split[0]
    headers_raw = split[1:]

    split_status = statusline.split(b' ')
    if len(split_status) != 3: raise Exception('Badly formatted request line')

    return {'method'  : split_status[0].decode('utf8'),
            'path'    : split_status[1].decode('utf8'),
            'headers'  : parse_headers(headers_raw)}

#=====================================================================
class read_body:
    def __init__ (self, reader, body_length: int, body_partial: bytes):
        self.reader       = reader
        self.body_length  = body_length
        self.body_partial = body_partial
        self.have_read    = 0

    def read(self, length = None) -> Union[bytes, None]:
        if self.have_read >= self.body_length: return None

        if length is None: length = self.body_length - self.have_read

        retbuffer: bytes
        if len(self.body_partial) > 0:
            retbuffer = self.body_partial[0:length]
            self.body_partial = self.body_partial[length:]

        else:
            retbuffer = self.reader(length)

        self.have_read += len(retbuffer)
        return retbuffer

    def read_all(self) -> bytes:
        retbuffer: bytes = b''

        while True:
            chunk = self.read(1000 * 1000)
            if chunk is None: break
            retbuffer += chunk

        return retbuffer

    def dump(self):
        """ Read whole body and discard it """

        while True:
            res = self.read(10000)
            if res is None: break
