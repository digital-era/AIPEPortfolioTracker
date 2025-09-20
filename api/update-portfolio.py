# File: api/update-portfolio.py
# Vercel 无服务器函数，CORS 处理与可运行的 trigger.py 相同

from http.server import BaseHTTPRequestHandler
import json
import base64
from github import Github, GithubException
import os
from datetime import datetime

class handler(BaseHTTPRequestHandler):

    # 使用与 trigger.py 相同的 ALLOWED_ORIGIN 逻辑
    # 您可以在 Vercel 中将其设置为环境变量以获得灵活性
    ALLOWED_ORIGIN = os.environ.get('ALLOWED_ORIGIN', "https://digital-era.github.io")

    def _set_cors_headers(self):
        """
        设置响应的 CORS 头部。
        """
        self.send_header('Access-Control-Allow-Origin', self.ALLOWED_ORIGIN)
        self.send_header('Access-Control-Allow-Methods', 'POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        # 添加此头部以处理凭据（如果需要）
        self.send_header('Access-Control-Allow-Credentials', 'true')
        # 为预检请求添加缓存（可选）
        self.send_header('Access-Control-Max-Age', '3600')

    def _set_headers(self, status_code=200):
        """
        设置 HTTP 响应头部，包括 CORS 头部。
        """
        self.send_response(status_code)
        self.send_header('Content-type', 'application/json')
        self._set_cors_headers()
        self.end_headers()

    def do_OPTIONS(self):
        """
        处理 CORS 预检请求，与 trigger.py 完全相同。
        """
        self._set_headers(200)
        self.wfile.write(b'')  # 为预检响应发送空主体

    def do_POST(self):
        try:
            # --- 1. 从请求中获取并解码 Excel 数据 ---
            content_length = int(self.headers.get('Content-Length', 0))
            if content_length == 0:
                 self._set_headers(400)
                 self.wfile.write(json.dumps({"error": "请求体为空。"}).encode('utf-8'))
                 return

            post_data = self.rfile.read(content_length)
            body = json.loads(post_data)

            if 'portfolioData' not in body:
                raise ValueError("请求体中缺少 'portfolioData'")
            
            excel_b64_string = body['portfolioData']
            excel_content = base64.b64decode(excel_b64_string)

            # --- 2. 配置 GitHub 访问 ---
            github_token = os.environ.get('GITHUB_TOKEN')
            repo_owner = os.environ.get('GITHUB_REPO_OWNER')
            repo_pro = os.environ.get('GITHUB_REPO_NAME')
            
            if not all([github_token, repo_owner, repo_pro]):
                raise ConnectionError("服务器配置不完整。缺少必需的 GitHub 环境变量。")

            repo_name = f"{repo_owner}/{repo_pro}"
            g = Github(github_token)
            repo = g.get_repo(repo_name)
            
            # --- 3. 准备直接提交到主分支 ---
            file_path = 'data/AIPEPortfolio_new.xlsx'
            commit_message = f"chore: 通过 Web UI 更新投资组合数据于 {datetime.now().strftime('%Y-%m-%d %H:%M')}"
            
            # --- 4. 检查文件是否存在以决定是创建还是更新 ---
            try:
                contents = repo.get_contents(file_path, ref="main")
                repo.update_file(
                    path=contents.path,
                    message=commit_message,
                    content=excel_content,
                    sha=contents.sha,
                    branch="main"
                )
                action = "更新"
            except GithubException as e:
                if e.status == 404:
                    repo.create_file(
                        path=file_path,
                        message=commit_message,
                        content=excel_content,
                        branch="main"
                    )
                    action = "创建"
                else:
                    raise e

            # --- 5. 响应客户端 ---
            self._set_headers(200)
            response_body = {"message": f"成功在主分支上{action}了 '{file_path}'。CI/CD 将现在开始处理。"}
            self.wfile.write(json.dumps(response_body).encode('utf-8'))

        except (ValueError, KeyError, ConnectionError) as e:
            self._set_headers(400)
            self.wfile.write(json.dumps({"error": str(e)}).encode('utf-8'))
        except Exception as e:
            self._set_headers(500)
            self.wfile.write(json.dumps({"error": f"发生了意外的服务器错误: {str(e)}"}).encode('utf-8'))
