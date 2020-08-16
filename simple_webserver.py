# Simple Web Server
# Read resources (xx.html files) from current directory then respond to client.
# Handling POST requests is same as GET.

import os
from http.server import HTTPServer, SimpleHTTPRequestHandler


class SimpleWebServerRequestHandler(SimpleHTTPRequestHandler):
    """Simple HTTP request handler.

    Handles GET, HEAD, POST requests.

    """

    def do_POST(self):
        self.do_GET()


os.chdir('.')

server_object = HTTPServer(server_address=('', 8080), RequestHandlerClass=SimpleWebServerRequestHandler)

# Start the web server
server_object.serve_forever()
