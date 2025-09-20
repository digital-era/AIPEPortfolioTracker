# File: api/update-portfolio.py
# This is a Vercel Serverless Function, with CORS handling identical to the working trigger.py

from http.server import BaseHTTPRequestHandler
import json
import base64
from github import Github, GithubException
import os
from datetime import datetime

class handler(BaseHTTPRequestHandler):

    # Use the same ALLOWED_ORIGIN logic as trigger.py
    # You can set this as an env var in Vercel for flexibility
    ALLOWED_ORIGIN = os.environ.get('ALLOWED_ORIGIN', "https://digital-era.github.io")

    def _set_headers(self, status_code=200):
        """
        Sets the HTTP response headers, including CORS headers.
        """
        self.send_response(status_code)
        self.send_header('Content-type', 'application/json')
        
        # --- CORS Headers (copied from trigger.py) ---
        self.send_header('Access-Control-Allow-Origin', self.ALLOWED_ORIGIN)
        self.send_header('Access-Control-Allow-Methods', 'POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

    def do_OPTIONS(self):
        """
        Handles CORS preflight requests, exactly like trigger.py.
        """
        self._set_headers(200)
        self.wfile.write(b'') # Send an empty body for the preflight response

    def do_POST(self):
        try:
            # --- 1. Get and decode the Excel data from the request ---
            content_length = int(self.headers.get('Content-Length', 0))
            if content_length == 0:
                 self._set_headers(400)
                 self.wfile.write(json.dumps({"error": "Request body is empty."}).encode('utf-8'))
                 return

            post_data = self.rfile.read(content_length)
            body = json.loads(post_data)

            if 'portfolioData' not in body:
                raise ValueError("Missing 'portfolioData' in request body")
            
            excel_b64_string = body['portfolioData']
            excel_content = base64.b64decode(excel_b64_string)

            # --- 2. Configure GitHub access ---
            github_token = os.environ.get('GITHUB_TOKEN')
            repo_owner = os.environ.get('GITHUB_REPO_OWNER')
            repo_pro = os.environ.get('GITHUB_REPO_NAME')
            
            if not all([github_token, repo_owner, repo_pro]):
                raise ConnectionError("Server configuration is incomplete. Required GitHub environment variables are missing.")

            repo_name = f"{repo_owner}/{repo_pro}"
            g = Github(github_token)
            repo = g.get_repo(repo_name)
            
            # --- 3. Prepare to commit directly to the main branch ---
            file_path = 'data/AIPEPortfolio_new.xlsx'
            commit_message = f"chore: Update portfolio data via web UI on {datetime.now().strftime('%Y-%m-%d %H:%M')}"
            
            # --- 4. Check if the file exists to decide whether to create or update ---
            try:
                contents = repo.get_contents(file_path, ref="main")
                repo.update_file(
                    path=contents.path,
                    message=commit_message,
                    content=excel_content,
                    sha=contents.sha,
                    branch="main"
                )
                action = "updated"
            except GithubException as e:
                if e.status == 404:
                    repo.create_file(
                        path=file_path,
                        message=commit_message,
                        content=excel_content,
                        branch="main"
                    )
                    action = "created"
                else:
                    raise e

            # --- 5. Respond to the client ---
            self._set_headers(200)
            response_body = {"message": f"Successfully {action} '{file_path}' on the main branch. CI/CD will now take over."}
            self.wfile.write(json.dumps(response_body).encode('utf-8'))

        except (ValueError, KeyError, ConnectionError) as e:
            self._set_headers(400)
            self.wfile.write(json.dumps({"error": str(e)}).encode('utf-8'))
        except Exception as e:
            self._set_headers(500)
            self.wfile.write(json.dumps({"error": f"An unexpected server error occurred: {str(e)}"}).encode('utf-8'))
