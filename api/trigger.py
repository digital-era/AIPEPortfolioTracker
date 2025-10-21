# api/trigger.py
import os
import requests
from http.server import BaseHTTPRequestHandler
import json

class handler(BaseHTTPRequestHandler):

    ALLOWED_ORIGIN = "https://digital-era.github.io"

    def _set_headers(self, status_code=200, content_type='application/json'):
        self.send_response(status_code)
        self.send_header('Content-type', content_type)
        self.send_header('Access-Control-Allow-Origin', self.ALLOWED_ORIGIN)
        self.send_header('Access-Control-Allow-Methods', 'POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.send_header('Access-Control-Max-Age', '86400')
        self.end_headers()

    def do_OPTIONS(self):
        self._set_headers(200)
        self.wfile.write(b'')

    def do_POST(self):
        token = os.environ.get('GITHUB_TOKEN')
        repo_owner = os.environ.get('GITHUB_REPO_OWNER')
        repo_name = os.environ.get('GITHUB_REPO_NAME')
        
        if not all([token, repo_owner, repo_name]):
            self._set_headers(500)
            response = {"error": "Server configuration is incomplete. Required environment variables are missing."}
            self.wfile.write(json.dumps(response).encode('utf-8'))
            return

        content_length = int(self.headers.get('Content-Length', 0))
        post_data_raw = self.rfile.read(content_length)
        post_data = {}
        if post_data_raw:
            try:
                post_data = json.loads(post_data_raw)
            except json.JSONDecodeError:
                self._set_headers(400)
                response = {"error": "Invalid JSON format in request body."}
                self.wfile.write(json.dumps(response).encode('utf-8'))
                return

        # =========================================================
        # # #  修改点 1: 获取并验证 list_type 参数
        # =========================================================
        list_type = post_data.get('list_type')
        # 验证 list_type 是否存在且有效
        if not list_type or list_type not in ['a_shares', 'hk_shares', 'etf']:
            self._set_headers(400) # Bad Request
            response = {
                "error": "Missing or invalid 'list_type' in request body.",
                "details": "It must be one of 'a_shares', 'hk_shares', or 'etf'."
            }
            self.wfile.write(json.dumps(response).encode('utf-8'))
            return
        # =========================================================

        workflow_file_name = "main.yml" 
        branch = "main"
        url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/actions/workflows/{workflow_file_name}/dispatches"

        headers = {
            "Accept": "application/vnd.github.v3+json",
            "Authorization": f"token {token}"
        }

        # =========================================================
        # # #  修改点 2: 将 list_type 添加到 workflow_inputs 中
        # =========================================================
        # 基础 inputs，现在包含必需的 list_type
        workflow_inputs = {
            "trigger_source": "api_call",
            "list_type": list_type
        }
        # =========================================================
        
        # 检查并添加 dynamiclist (A股)、dynamicHKlist (港股) 和 dynamicETFlist (ETF)
        dynamic_list_a = post_data.get('dynamiclist')
        if dynamic_list_a and isinstance(dynamic_list_a, list):
            workflow_inputs['dynamiclist'] = json.dumps(dynamic_list_a)

        dynamic_list_hk = post_data.get('dynamicHKlist')
        if dynamic_list_hk and isinstance(dynamic_list_hk, list):
            workflow_inputs['dynamicHKlist'] = json.dumps(dynamic_list_hk)

        dynamic_list_etf = post_data.get('dynamicETFlist')
        if dynamic_list_etf and isinstance(dynamic_list_etf, list):
            workflow_inputs['dynamicETFlist'] = json.dumps(dynamic_list_etf)

        data = {
            "ref": branch,
            "inputs": workflow_inputs
        }
        
        try:
            res = requests.post(url, headers=headers, json=data)

            if res.status_code == 204:
                self._set_headers(202)
                response = {
                    "message": "Workflow triggered successfully.",
                    "details": f"Check the 'Actions' tab in your GitHub repository '{repo_owner}/{repo_name}' for progress.",
                    "sent_inputs": workflow_inputs
                }
                self.wfile.write(json.dumps(response).encode('utf-8'))
            else:
                self._set_headers(res.status_code)
                response = {
                    "error": "Failed to trigger GitHub workflow.",
                    "github_response": res.text
                }
                self.wfile.write(json.dumps(response).encode('utf-8'))

        except Exception as e:
            self._set_headers(500)
            response = {"error": f"An internal error occurred: {str(e)}"}
            self.wfile.write(json.dumps(response).encode('utf-8'))
            
        return

    def do_GET(self):
        self._set_headers(405)
        self.send_header('Allow', 'POST, OPTIONS')
        self.end_headers()
        response = {"error": "Method not allowed. Please use a POST request to trigger the workflow."}
        self.wfile.write(json.dumps(response).encode('utf-8'))
        return
