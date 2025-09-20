# File: api/update-portfolio.py
# Vercel 无服务器函数，CORS 处理与可运行的 trigger.py 相同

import os
import json
import base64
from github import Github, GithubException
from datetime import datetime

ALLOWED_ORIGIN = os.environ.get("ALLOWED_ORIGIN", "https://digital-era.github.io")

def _cors_headers():
    return {
        "Access-Control-Allow-Origin": ALLOWED_ORIGIN,
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
        "Access-Control-Allow-Credentials": "true",
        "Access-Control-Max-Age": "3600",
    }

def handler(request):
    # --- 1. 处理 CORS 预检请求 ---
    if request.method == "OPTIONS":
        return {
            "statusCode": 200,
            "headers": _cors_headers(),
            "body": ""
        }

    # --- 2. 处理 POST 请求 ---
    if request.method == "POST":
        try:
            body = request.json()
            if "portfolioData" not in body:
                return {
                    "statusCode": 400,
                    "headers": _cors_headers(),
                    "body": json.dumps({"error": "请求体中缺少 'portfolioData'"})
                }

            excel_b64_string = body["portfolioData"]
            excel_content = base64.b64decode(excel_b64_string)

            # --- 3. 配置 GitHub 访问 ---
            github_token = os.environ.get("GITHUB_TOKEN")
            repo_owner = os.environ.get("GITHUB_REPO_OWNER")
            repo_pro = os.environ.get("GITHUB_REPO_NAME")

            if not all([github_token, repo_owner, repo_pro]):
                return {
                    "statusCode": 500,
                    "headers": _cors_headers(),
                    "body": json.dumps({"error": "服务器配置不完整。缺少必需的 GitHub 环境变量。"})
                }

            repo_name = f"{repo_owner}/{repo_pro}"
            g = Github(github_token)
            repo = g.get_repo(repo_name)

            # --- 4. 提交文件 ---
            file_path = "data/AIPEPortfolio_new.xlsx"
            commit_message = f"chore: 通过 Web UI 更新投资组合数据于 {datetime.now().strftime('%Y-%m-%d %H:%M')}"

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

            return {
                "statusCode": 200,
                "headers": _cors_headers(),
                "body": json.dumps({
                    "message": f"成功在主分支上{action}了 '{file_path}'。CI/CD 将现在开始处理。"
                })
            }

        except Exception as e:
            return {
                "statusCode": 500,
                "headers": _cors_headers(),
                "body": json.dumps({"error": f"发生了意外的服务器错误: {str(e)}"})
            }

    # --- 3. 其他方法不允许 ---
    return {
        "statusCode": 405,
        "headers": _cors_headers(),
        "body": json.dumps({"error": "Method Not Allowed"})
    }
