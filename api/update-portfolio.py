# File: api/update-portfolio.py
# This is a Vercel Serverless Function (Modified to commit directly to main)

from http.server import BaseHTTPRequestHandler
import json
import base64
from github import Github, GithubException
import os
from datetime import datetime

class handler(BaseHTTPRequestHandler):

    def do_POST(self):
        try:
            # --- 1. Get and decode the Excel data from the request ---
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            body = json.loads(post_data)

            if 'portfolioData' not in body:
                raise ValueError("Missing 'portfolioData' in request body")
            
            excel_b64_string = body['portfolioData']
            excel_content = base64.b64decode(excel_b64_string)

            # --- 2. Configure GitHub access ---
            # IMPORTANT: Set these as Environment Variables in your Vercel project
            github_token = os.environ.get('GITHUB_TOKEN')
            repo_owner = os.environ.get('GITHUB_REPO_OWNER')
            repo_pro = os.environ.get('GITHUB_REPO_NAME')
            repo_name = f"{repo_owner}/{repo_pro}"
            
            if not github_token:
                raise ConnectionError("GITHUB_TOKEN environment variable is not set.")

            g = Github(github_token)
            repo = g.get_repo(repo_name)
            
            # --- 3. Prepare to commit directly to the main branch ---
            file_path = 'data/AIPEPortfolio_new.xlsx'
            commit_message = f"chore: Update portfolio data via web UI on {datetime.now().strftime('%Y-%m-%d %H:%M')}"
            
            # --- 4. Check if the file exists to decide whether to create or update ---
            try:
                # Get the existing file to get its SHA, required for an update
                contents = repo.get_contents(file_path, ref="main")
                # If the file exists, update it
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
                    # If the file does not exist, create it
                    repo.create_file(
                        path=file_path,
                        message=commit_message,
                        content=excel_content,
                        branch="main"
                    )
                    action = "created"
                else:
                    # Reraise other GitHub-related errors
                    raise e

            # --- 5. Respond to the client ---
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response_body = {"message": f"Successfully {action} '{file_path}' on the main branch. CI/CD will now take over."}
            self.wfile.write(json.dumps(response_body).encode('utf-8'))

        except (ValueError, KeyError, ConnectionError) as e:
            self.send_response(400)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({"error": str(e)}).encode('utf-8'))
        except Exception as e:
            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({"error": f"An unexpected server error occurred: {e}"}).encode('utf-8'))
