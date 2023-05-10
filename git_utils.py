import os
from google.cloud import bigquery
from google.cloud import storage
from github import Github
import json
import requests
from datetime import datetime as d


def clone_github_repo(token):
    g = Github(token)
    for repo in g.get_user().get_repos():
        print(repo.full_name)
    repo = g.get_repo('RealistAI/UC4_SQL')
    content = repo.get_contents('')
    while content:
        file_content = content.pop(0)
        print(dir(file_content))
        if file_content.type == 'dir':
            content.extend(repo.get_contents(file_content.path))
        else:
            file_name = f'git_repo/{file_content.name}'
            with open (file_name, 'w') as file:
                file.write(file_content.content)




if __name__ == "__main__":
    clone_github_repo()
