# File: api/update-portfolio.py
# 这是一个用于诊断的、绝对最小化的版本
# 它不依赖 PyGithub，只依赖 Python 内置库

from http.server import BaseHTTPRequestHandler
import json

class handler(BaseHTTPRequestHandler):

    def _send_response(self, status_code, data):
        """发送响应"""
        self.send_response(status_code)
        self.send_header('Content-type', 'application/json')
        # 强制添加最开放的 CORS 头部用于测试
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', '*')
        self.send_header('Access-Control-Allow-Headers', '*')
        self.end_headers()
        self.wfile.write(json.dumps(data).encode('utf-8'))

    def do_OPTIONS(self):
        """响应预检请求"""
        self.send_response(204)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', '*')
        self.send_header('Access-Control-Allow-Headers', '*')
        self.end_headers()

    def do_POST(self):
        """响应 POST 请求"""
        # 只打印一句话，然后返回成功
        print("Minimal update-portfolio.py was successfully called!")
        message = {"message": "Hello from the minimal update-portfolio.py!"}
        self._send_response(200, message)
        
    def do_GET(self):
        """响应 GET 请求"""
        print("Minimal update-portfolio.py was successfully called via GET!")
        message = {"message": "GET request received by minimal update-portfolio.py!"}
        self._send_response(200, message)
