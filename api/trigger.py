# api/trigger.py
import os
import requests
from http.server import BaseHTTPRequestHandler
import json

class handler(BaseHTTPRequestHandler):

    # 定义允许的来源 (Origin)
    # 根据您提供的错误信息，您的前端是 'https://digital-era.github.io'
    ALLOWED_ORIGIN = "https://digital-era.github.io"

    def _set_headers(self, status_code=200, content_type='application/json'):
        """
        设置HTTP响应头部，包括CORS相关头部。
        """
        self.send_response(status_code)
        self.send_header('Content-type', content_type)
        
        # --- CORS Headers ---
        self.send_header('Access-Control-Allow-Origin', self.ALLOWED_ORIGIN)
        self.send_header('Access-Control-Allow-Methods', 'POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.send_header('Access-Control-Max-Age', '86400') # 24小时
        
        self.end_headers()

    def do_OPTIONS(self):
        """
        处理 CORS 预检请求。
        """
        self._set_headers(200)
        self.wfile.write(b'')

    def do_POST(self):
        """
        处理 POST 请求，触发 GitHub Workflow。
        """
        # --- 从 Vercel 环境变量中获取配置 ---
        token = os.environ.get('GITHUB_TOKEN')
        repo_owner = os.environ.get('GITHUB_REPO_OWNER')
        repo_name = os.environ.get('GITHUB_REPO_NAME')
        
        # 检查必要的环境变量是否存在
        if not all([token, repo_owner, repo_name]):
            self._set_headers(500)
            response = {"error": "Server configuration is incomplete. Required environment variables are missing."}
            self.wfile.write(json.dumps(response).encode('utf-8'))
            return

        # --- 解析传入的POST请求体 ---
        content_length = int(self.headers.get('Content-Length', 0))
        post_data_raw = self.rfile.read(content_length)
        post_data = {}
        if post_data_raw:
            try:
                post_data = json.loads(post_data_raw)
            except json.JSONDecodeError:
                self._set_headers(400) # Bad Request
                response = {"error": "Invalid JSON format in request body."}
                self.wfile.write(json.dumps(response).encode('utf-8'))
                return

        # --- 调用 GitHub API ---
        workflow_file_name = "main.yml" 
        branch = "main"
        url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/actions/workflows/{workflow_file_name}/dispatches"

        headers = {
            "Accept": "application/vnd.github.v3+json",
            "Authorization": f"token {token}"
        }

        # --- 构建包含动态参数的 inputs ---
        # 基础 inputs
        workflow_inputs = {
            "trigger_source": "api_call"
        }
        
        # 检查并添加 dynamiclist (A股)、dynamicHKlist (港股) 和 dynamicETFlist (ETF)
        # GitHub Actions 的 inputs 只接受字符串，所以我们将列表转换为 JSON 字符串
        dynamic_list_a = post_data.get('dynamiclist')
        if dynamic_list_a and isinstance(dynamic_list_a, list):
            workflow_inputs['dynamiclist'] = json.dumps(dynamic_list_a)

        dynamic_list_hk = post_data.get('dynamicHKlist')
        if dynamic_list_hk and isinstance(dynamic_list_hk, list):
            workflow_inputs['dynamicHKlist'] = json.dumps(dynamic_list_hk)

        # =========================================================
        # #  新增的代码块 
        # =========================================================
        # 新增：检查并添加 dynamicETFlist (ETF)
        dynamic_list_etf = post_data.get('dynamicETFlist')
        if dynamic_list_etf and isinstance(dynamic_list_etf, list):
            workflow_inputs['dynamicETFlist'] = json.dumps(dynamic_list_etf)
        # =========================================================
        #  #新增的代码块 
        # =========================================================

        data = {
            "ref": branch,
            "inputs": workflow_inputs
        }
        
        try:
            # 发送 POST 请求到 GitHub API
            res = requests.post(url, headers=headers, json=data)

            # GitHub 成功接收请求后会返回 204 No Content
            if res.status_code == 204:
                self._set_headers(202) # 202 Accepted
                response = {
                    "message": "Workflow triggered successfully.",
                    "details": f"Check the 'Actions' tab in your GitHub repository '{repo_owner}/{repo_name}' for progress.",
                    "sent_inputs": workflow_inputs # 返回发送的参数，便于调试
                }
                self.wfile.write(json.dumps(response).encode('utf-8'))
            else:
                self._set_headers(res.status_code)
                response = {
                    "error": "Failed to trigger GitHub workflow.",
                    "github_response": res.text # 使用 res.text 获取更详细的错误信息
                }
                self.wfile.write(json.dumps(response).encode('utf-8'))

        except Exception as e:
            self._set_headers(500)
            response = {"error": f"An internal error occurred: {str(e)}"}
            self.wfile.write(json.dumps(response).encode('utf-8'))
            
        return

    def do_GET(self):
        self._set_headers(405) # 405 Method Not Allowed
        self.send_header('Allow', 'POST, OPTIONS')
        self.end_headers()
        response = {"error": "Method not allowed. Please use a POST request to trigger the workflow."}
        self.wfile.write(json.dumps(response).encode('utf-8'))
        return
