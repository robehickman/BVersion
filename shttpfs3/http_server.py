import socket, _thread, threading
from http.server import BaseHTTPRequestHandler
from io import BytesIO, TextIOWrapper
#=============================================
class HTTPRequest(BaseHTTPRequestHandler):
    def __init__(self, request_text):
        self.rfile = BytesIO(request_text)
        self.raw_requestline = self.rfile.readline()
        self.error_code = self.error_message = None
        self.parse_request()

    def send_error(self, code, message):
        self.error_code = code
        self.error_message = message

#=====================
class Request:
    def __init__ (self, remote_addr: str, remote_port: int, uri: str, headers, body):
        self.remote_addr = remote_addr
        self.remote_port = remote_port
        self.uri = uri
        self.headers = headers
        self.body    = body

    def get_json(self):
        return json.loads(self.body.read())

#=====================
class Responce:
    def __init__ (self, headers = None, body = " "):
        if headers is None: headers = {}
        self.headers = headers
        self.body    = body

#=====================
class read_body:
    def __init__ (self, reader, body_length: int, body_partial: bytes): 
        self.reader       = reader
        self.body_length  = body_length
        self.body_partial = body_partial
        self.have_read    = 0
            
    def __call__(self, length = None):
        if self.have_read >= self.body_length: return None
        if length is None: length = self.body_length
        left_to_read = self.body_length - self.have_read
        if left_to_read < length: length = left_to_read
        if len(self.body_partial) < length:
            self.body_partial += self.reader(length - len(self.body_partial))          
        retbuffer = self.body_partial[0:length]
        self.body_partial = self.body_partial[length:]
        self.have_read += length
        return retbuffer


#=============================================
def HTTPServer(host, port, connection_handler):
    def handle_connection(c, addr):
        try:
            while True:

                data = b""
        
                # read request preamble
                while True:
                    data += c.recv(1024) 
                    if b"\r\n\r\n" in data: break
                preamble, body_partial = data.split(b"\r\n\r\n")

                # parse the header
                request = HTTPRequest(preamble)
                if request.error_code is not None:
                    print(self.error_message)
                    break

                if request.command.lower() != 'post':
                    print('error parsing request')
                    break
                    
                request_headers = {k.lower() : v for k,v in dict(request.headers).items()}

                # handle the request
                body_length = int(request_headers['content-length'])
                body_reader = read_body(c.recv, body_length, body_partial)
                rq = Request(addr[0], addr[1], request.path, request_headers, body_reader)
                rsp: Responce = connection_handler(rq)

                # generate client responce
                responce_headers =  b"HTTP/1.1 200 OK\r\n"
                responce_headers += b"Connection: Keep-Alive\r\n"
                responce_headers += b"Content-Length: 0\r\n"
                
                for k, v in rsp.headers.items():
                    if isinstance(k, str): k=k.encode('utf8')
                    if isinstance(v, str): v=v.encode('utf8')
                    responce_headers += k + b':' + v + b'\r\n'
                    
                responce_headers += b"\r\n"
                c.send(responce_headers)

                if isinstance(rsp.body, TextIOWrapper):
                    client_socket.sendfile(rsp.body, 0)
                else:
                    pass
                    #c.send(rsp.body)
                
                break
        except:
            c.close()
            raise

        # connection closed 
        c.close() 

    #============
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
    s.bind((host, port)) 
    print("socket bound to port", port) 
    
    # put the socket into listening mode 
    s.listen(5) 
    print("socket is listening") 

    try:
        while True: 
            c, addr = s.accept() 
            print('Connected to:', addr[0], ':', addr[1]) 
    
            # Start a new thread and return its identifier 
            _thread.start_new_thread(handle_connection, (c, addr))
    except:
        s.close()
        raise

    s.close() 


