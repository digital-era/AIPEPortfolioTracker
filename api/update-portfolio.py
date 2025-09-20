# File: api/update-portfolio.py
# This is a Vercel Serverless Function

from http.server import BaseHTTPRequestHandler
import json
import base64
from github import Github
from github import InputGitTreeElement
import os
import uuid
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
            repo_pro = os.environ.get('GITHUB_REPO_NAME') # e.g., "your-username/your-repo"
            repo_name = repo_owner + "/" +  repo_pro
            
            if not github_token:
                raise ConnectionError("GITHUB_TOKEN environment variable is not set.")

            g = Github(github_token)
            repo = g.get_repo(repo_name)
            
            # --- 3. Create a new branch to commit the changes ---
            # This is safer than committing directly to main
            source_branch = repo.get_branch('main')
            new_branch_name = f"update-portfolio-{datetime.now().strftime('%Y%m%d%H%M%S')}-{str(uuid.uuid4())[:4]}"
            
            repo.create_git_ref(ref=f'refs/heads/{new_branch_name}', sha=source_branch.commit.sha)

            # --- 4. Upload the new Excel file to the new branch ---
            file_path = 'data/AIPEPortfolio_new.xlsx' # Name of the temporary file to be merged by the Action
            commit_message = f"chore: Update portfolio data via web UI on {datetime.now().strftime('%Y-%m-%d %H:%M')}"
            
            # This will create or update the file
            repo.create_file(
                path=file_path,
                message=commit_message,
                content=excel_content,
                branch=new_branch_name
            )

            # --- 5. Respond to the client ---
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response_body = {"message": f"Successfully pushed new portfolio to branch '{new_branch_name}'. CI/CD will now take over."}
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
