from typing import List, Dict, Union
from typing_extensions import TypedDict

#=====================================================================
def generate_headers(headers: Dict[str, str]) -> bytes:
    res: bytes = b''
    for k,v in headers.items():
        if isinstance(k, str): k = k.encode('utf8')
        if isinstance(v, str): v = v.encode('utf8')

        res += k + b': ' + v + b'\r\n'
    return res

#=====================================================================
class httpPreamble(TypedDict):
    protocol: str
    status:   List[str]
    heders:   Dict[str, str]

#=====================================================================
def parse_http_preamble(preamble: bytes) -> httpPreamble:
    split = preamble.split(b"\r\n")
    statusline = split[0]
    headers_raw = split[1:]

    split_status = statusline.split(b' ')
    if len(split_status) != 3: raise Exception('Badly formatted status line')

    headers = {}
    for i in headers_raw:
        split_header = i.split(b':', 1)
        headers[split_header[0].strip().decode('utf8').lower()] = split_header[1].strip().decode('utf8')
    
    return {'protocol' : split_status[0],
            'status'   : split_status[1:2],
            'headers'  : headers}

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

    def dump(self):
        """ Read whole body and discard it """

        while True:
            res = self.read(10000)
            if res is None: break
