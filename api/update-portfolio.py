# File: api/update-portfolio.py
import os
import json
import base64
from github import Github, GithubException
from datetime import datetime

# 环境变量应该是你在 Vercel 项目设置中配置的
ALLOWED_ORIGIN = os.environ.get("ALLOWED_ORIGIN") 

def _cors_headers():
    # 确保 ALLOWED_ORIGIN 环境变量已设置，否则 CORS 会失败
    if not ALLOWED_ORIGIN:
        # 在服务器日志中打印警告，便于调试
        print("Warning: ALLOWED_ORIGIN environment variable is not set.")
        return {} # 返回空字典，这样更容易在浏览器中看到错误
        
    return {
        "Access-Control-Allow-Origin": ALLOWED_ORIGIN,
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
        "Access-Control-Allow-Credentials": "true",
        "Access-Control-Max-Age": "3600",
    }

def handler(request):
    # 打印请求方法，用于调试
    print(f"Request method: {request.method}")

    # --- 1. 处理 CORS 预检请求 ---
    # Vercel 的 Python 运行时，HTTP 方法是大写的
    if request.method == "OPTIONS":
        print("Handling OPTIONS preflight request.")
        return {
            "statusCode": 204,  # 对于预检请求，返回 204 No Content 更标准
            "headers": _cors_headers(),
            "body": ""
        }

    # --- 2. 处理 POST 请求 ---
    if request.method == "POST":
        print("Handling POST request.")
        try:
            # 尝试从 request 中获取 json 数据
            # Vercel 的请求对象有一个 .json() 方法
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
            repo_pro = os.environ.get("GITHUB_REPO_NAME") # 变量名修正，与代码一致

            if not all([github_token, repo_owner, repo_pro]):
                print("Error: Missing required GitHub environment variables.")
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
                    # 重新抛出其他 GitHub 异常
                    raise e

            return {
                "statusCode": 200,
                "headers": _cors_headers(),
                "body": json.dumps({
                    "message": f"成功在主分支上{action}了 '{file_path}'。CI/CD 将现在开始处理。"
                })
            }

        except Exception as e:
            print(f"An unexpected error occurred: {str(e)}")
            return {
                "statusCode": 500,
                "headers": _cors_headers(),
                "body": json.dumps({"error": f"发生了意外的服务器错误: {str(e)}"})
            }

    # --- 3. 其他方法不允许 ---
    print(f"Method {request.method} not allowed.")
    return {
        "statusCode": 405,
        "headers": _cors_headers(),
        "body": json.dumps({"error": "Method Not Allowed"})
    }
