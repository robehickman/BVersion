from poster.encode import multipart_encode
from poster.streaminghttp import register_openers
import urllib2

class do_request:
############################################################################################
    def __init__(self, server_base_url):
        "Configure the servers base URL"
        self.server_base_url = server_base_url

############################################################################################
    def request_raw(self, url, data):
        "Send a HTTP request to the configured server and return the result object"
        datagen, headers = multipart_encode(data)
        request = urllib2.Request(self.server_base_url + url, datagen, headers)
        result = urllib2.urlopen(request)
        return result

############################################################################################
    def request_full(self, url, data, gen = False):
        "Send a HTTP request to the configured server and return the result body and headers"
        result = self.request_raw(url, data)

        if gen == False:
            return (result.read(), result.info())

        def writer(path):
            with open(path, 'w') as f:
                while True:
                    Chunk = result.read(1000 * 1000)
                    if not Chunk: break
                    f.write(Chunk)

        return (writer, result.info())

        

