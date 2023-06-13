import subprocess
import shlex
import translate_sql as ts
import config

def submit_pull_request(repo_owner, repo_name, base_branch, head_branch, title, body, github_token):
    # Construct the curl command with the necessary parameters
    curl_command = f'curl -X POST -H "Authorization: token {github_token}" \
                    -d \'{{"title":"{title}","body":"{body}","head":"{head_branch}","base":"{base_branch}"}}\' \
                    https://api.github.com/repos/{repo_owner}/{repo_name}/pulls'

    # Split the command into individual arguments
    args = shlex.split(curl_command)

    try:
        # Execute the curl command
        subprocess.run(args, check=True)
        print("Pull request submitted successfully!")
    except subprocess.CalledProcessError as e:
        print(f"Error occurred while submitting the pull request: {e}")

repo_owner = config.REPO_OWNER
repo_name = config.UC4_SQL_REPO_NAME
base_branch = config.BASE_REPO_BRANCH
head_branch = ts.main()
title = "Test Pull Request"
body = "Pull request for testing purposes"
github_token = config.TOKEN

submit_pull_request(repo_owner, repo_name, base_branch, head_branch, title, body, github_token)


