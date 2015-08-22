#!/usr/bin/env python
#
# Simple asynchronous HTTP proxy with tunnelling (CONNECT).
#
# GET/POST proxying based on
# http://groups.google.com/group/python-tornado/msg/7bea08e7a049cf26
#
# Copyright (C) 2012 Senko Rasic <senko.rasic@dobarkod.hr>
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

#https://github.com/senko/tornado-proxy/zipball/master

import sys
import socket
import urlparse
import time
from optparse import OptionParser

import tornado.httpserver
import tornado.ioloop
import tornado.iostream
import tornado.web
import tornado.httpclient

import cache
from utils import Counter

DEFAULT_THROTTLE = 1

cache = cache.Cache(ns='cache')
stats = Counter()
stats.statsd.host = 'stats.api.cloudlibs.com'
stats = stats.statsd
host_name = socket.gethostname().replace('proxy', 'crawling.')

__all__ = ['ProxyHandler', 'run_proxy']


class ProxyHandler(tornado.web.RequestHandler):
    SUPPORTED_METHODS = ['GET', 'POST', 'CONNECT']

    @tornado.web.asynchronous
    def get(self):

        def handle_response(response):
            if response.error and not isinstance(response.error,
                    tornado.httpclient.HTTPError):
                self.set_status(500)
                self.write('Internal server error:\n' + str(response.error))
                self.finish()
            else:
                self.set_status(response.code)
                for header in ('Date', 'Cache-Control', 'Server',
                        'Content-Type', 'Location'):
                    v = response.headers.get(header)
                    if v:
                        self.set_header(header, v)
                if response.body:
                    self.write(response.body)
                self.finish()

        req = tornado.httpclient.HTTPRequest(url=self.request.uri,
            method=self.request.method, body=self.request.body,
            headers=self.request.headers, follow_redirects=False,
            allow_nonstandard_methods=True)

        stats.update_stats('services.juicer2.stats.proxy.%s.requests'%host_name)
        netloc = urlparse.urlparse(self.request.uri).netloc
        has_recent_activity = cache.get(netloc)
        throttle = DEFAULT_THROTTLE + time.time() if has_recent_activity else time.time()
        stats.update_stats('services.juicer2.stats.proxy.%s.throttling'%host_name, 1 if has_recent_activity else 0)

        ioloop = tornado.ioloop.IOLoop.instance()
        client = tornado.httpclient.AsyncHTTPClient()
        fn = lambda: client.fetch(req, handle_response)

        cache.set(netloc, True, 1)

        try:
            ioloop.add_timeout(throttle, fn)
        except tornado.httpclient.HTTPError, e:
            if hasattr(e, response) and e.response:
                self.handle_response(e.response)
            else:
                self.set_status(500)
                self.write('Internal server error:\n' + str(e))
                self.finish()

    @tornado.web.asynchronous
    def post(self):
        return self.get()

    @tornado.web.asynchronous
    def connect(self):
        host, port = self.request.uri.split(':')
        client = self.request.connection.stream

        def read_from_client(data):
            upstream.write(data)

        def read_from_upstream(data):
            client.write(data)

        def client_close(_dummy):
            upstream.close()

        def upstream_close(_dummy):
            client.close()

        def start_tunnel():
            client.read_until_close(client_close, read_from_client)
            upstream.read_until_close(upstream_close, read_from_upstream)
            client.write('HTTP/1.0 200 Connection established\r\n\r\n')

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        upstream = tornado.iostream.IOStream(s)

        stats.update_stats('services.juicer2.stats.proxy.%s.requests'%host_name)
        has_recent_activity = cache.get(host)
        throttle = DEFAULT_THROTTLE + time.time() if has_recent_activity else time.time()
        stats.update_stats('services.juicer2.stats.proxy.%s.throttling'%host_name, 1 if has_recent_activity else 0)

        ioloop = tornado.ioloop.IOLoop.instance()
        fn = lambda: upstream.connect((host, int(port)), start_tunnel)
        cache.set(host, True, 1)

        ioloop.add_timeout(throttle, fn)

def run_proxy(port, start_ioloop=True):
    """
    Run proxy on the specified port. If start_ioloop is True (default),
    the tornado IOLoop will be started immediately.
    """
    app = tornado.web.Application([
        (r'.*', ProxyHandler),
    ])
    app.listen(port)
    ioloop = tornado.ioloop.IOLoop.instance()
    if start_ioloop:
        ioloop.start()

if __name__ == '__main__':
    parser = OptionParser(usage=u'usage: %prog [options]')
    parser.add_option('-p', '--port', dest='port', action='store', type='int', default=62986,
                              help='Listening port')
    (options,args) = parser.parse_args()

    print "Starting HTTP proxy on port %d" % options.port
    run_proxy(options.port)
