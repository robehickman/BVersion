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
    def request(self, url, data):
        "Send a HTTP request to the configured server and return the result body"
        result = self.request_raw(url, data)
        return result.read()


############################################################################################
    def request_full(self, url, data):
        "Send a HTTP request to the configured server and return the result body and headers"
        result = self.request_raw(url, data)
        return (result.read(), result.info())

