#!/usr/bin/env python3
"""Local dev server for viewer.html — avoids CORS errors from file:// protocol."""
import http.server
import webbrowser
import threading

PORT = 8000

def open_browser():
    webbrowser.open(f"http://localhost:{PORT}/viewer.html")

threading.Timer(0.5, open_browser).start()
print(f"Serving at http://localhost:{PORT}/viewer.html  (Ctrl+C to stop)")
http.server.test(HandlerClass=http.server.SimpleHTTPRequestHandler, port=PORT)
