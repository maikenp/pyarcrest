import json
import logging
import ssl
from http.client import HTTPConnection, HTTPSConnection, RemoteDisconnected
from urllib.parse import urlencode, urlparse

from pyarcrest.errors import HTTPClientError

HTTP_BUFFER_SIZE = 2**23


# TODO: hardcoded timeout for http.client connection
# TODO: when required python version becomes 3.7 >=, use buffer size for HTTP
#       client
class HTTPClient:

    def __init__(self, url=None, host=None, port=None, proxypath=None, isHTTPS=False, logger=None):
        """Process parameters and create HTTP connection."""
        self.logger = logger
        if not self.logger:
            self.logger = logging.getLogger("null")
            if not self.logger.hasHandlers():
                self.logger.addHandler(logging.NullHandler())

        if url:
            parts = urlparse(url)

            if parts.scheme == "https":
                useHTTPS = True
            elif parts.scheme == "http":
                useHTTPS = False
            else:
                raise HTTPClientError("URL scheme not HTTP(S)")

            self.host = parts.hostname
            if self.host is None:
                raise HTTPClientError("No hostname in URL")

            self.port = parts.port

        else:
            self.host = host
            if self.host is None:
                raise HTTPClientError("No hostname parameter")

            useHTTPS = isHTTPS
            self.port = port

        if self.host is None:
            raise HTTPClientError("No hostname given")

        if proxypath is not None:
            if not useHTTPS:
                raise HTTPClientError("Cannot use proxy without HTTPS")
            else:
                context = ssl.SSLContext(ssl.PROTOCOL_TLS)
                context.load_cert_chain(proxypath, keyfile=proxypath)
        else:
            context = None

        if useHTTPS:
            if not self.port:
                self.port = 443
            self.conn = HTTPSConnection(self.host, port=self.port, context=context, timeout=600)
        else:
            if not self.port:
                self.port = 80
            self.conn = HTTPConnection(self.host, port=self.port, timeout=600)

        self.isHTTPS = useHTTPS
        self.proxypath = proxypath

    def request(self, method, endpoint, headers={}, token=None, jsonData=None, data=None, params={}):
        """Send request and retry on ConnectionErrors."""
        if token:
            headers['Authorization'] = f'Bearer {token}'

        if jsonData:
            body = json.dumps(jsonData).encode()
            headers['Content-Type'] = 'application/json'
        else:
            body = data

        for key, value in params.items():
            if isinstance(value, list):
                params[key] = ','.join([str(val) for val in value])

        query = ''
        if params:
            query = urlencode(params)

        if query:
            url = f'{endpoint}?{query}'
        else:
            url = endpoint

        try:
            self.logger.debug(f"{method} {url} headers={headers}")
            self.conn.request(method, url, body=body, headers=headers)
            resp = self.conn.getresponse()
        # TODO: should the request be retried for aborted connection by peer?
        except (RemoteDisconnected, BrokenPipeError, ConnectionAbortedError, ConnectionResetError):
            # retry request
            try:
                self.conn.request(method, url, body=body, headers=headers)
                resp = self.conn.getresponse()
            except:
                self.close()
                raise
        except:
            self.close()
            raise

        return resp

    def close(self):
        """Close connection."""
        self.conn.close()
