import os
import re
import logging
import requests
import boto3
from util import get_logger, get_api_key, check_aws_creds, get_git_creds, get_assignment, get_submission_dir  #, get_changed_files
from openai import OpenAI

logger = get_logger()
client = OpenAI(api_key=get_api_key())
assignment = get_assignment()
submission_dir = get_submission_dir()
git_token, repo, pr_number = get_git_creds()
s3_bucket = check_aws_creds()

code_snippet = """
`{file_name}`:

```python
{file_content}
```
"""


def download_from_s3(s3_bucket: str, s3_path: str, local_path: str):
    s3 = boto3.client('s3')
    try:
        s3.download_file(s3_bucket, s3_path, local_path)
    except Exception as e:
        raise Exception(f"Failed to download from S3: {e}")


def get_prompts(s3_solutions_dir: str, local_solutions_dir: str) -> str:
    s3_path = f"{s3_solutions_dir}/system_prompt.md"
    local_system_prompt_path = os.path.join(local_solutions_dir,
                                            'system_prompt.md')
    download_from_s3(s3_bucket, s3_path, local_system_prompt_path)
    if not os.path.exists(local_system_prompt_path):
        raise ValueError(f"Path does not exist: {local_system_prompt_path}")

    local_user_prompt_path = os.path.join(local_solutions_dir,
                                          'user_prompt.md')
    download_from_s3(s3_bucket, s3_path, local_user_prompt_path)
    if not os.path.exists(local_user_prompt_path):
        raise ValueError(f"Path does not exist: {local_user_prompt_path}")

    system_prompt = open(local_system_prompt_path, "r").read()
    user_prompt = open(local_user_prompt_path, "r").read()

    return system_prompt, user_prompt


def get_submissions(submission_dir: str) -> dict:
    submissions = {}
    try:
        jobs_dir = os.path.join(submission_dir, 'jobs')
        tests_dir = os.path.join(submission_dir, 'tests')
        jobs_files = [
            os.path.join('jobs', f) for f in os.listdir(jobs_dir)
            if os.path.isfile(os.path.join(jobs_dir, f))
        ]
        tests_files = [
            os.path.join('tests', f) for f in os.listdir(tests_dir)
            if os.path.isfile(os.path.join(tests_dir, f))
        ]
        submission_files = jobs_files + tests_files
    except FileNotFoundError:
        logger.error(f"Submission directory not found: {submission_dir}")
        return None

    logger.info(f"Submission files: {submission_files}")
    for sub_file in submission_files:
        file_path = os.path.join(submission_dir, sub_file)
        if os.path.isfile(file_path) and (
                re.match(r'jobs/job_.*\.py', sub_file)
                or re.match(r'tests/test_.*\.py', sub_file)):
            try:
                with open(file_path, "r") as file:
                    file_content = file.read()
                if re.search(r'\S', file_content):
                    submissions[file_path] = file_content
            except FileNotFoundError:
                logger.info(f"File not found: {file_path}")
                continue

    if not submissions:
        logger.warning('No submissions found')
        return None

    sorted_submissions = dict(sorted(submissions.items()))
    return sorted_submissions


def get_feedback(system_prompt: str, user_prompt: str) -> str:
    comment = ''
    response = client.chat.completions.create(
        model="gpt-4",
        messages=[
            {
                "role": "system",
                "content": system_prompt
            },
            {
                "role": "user",
                "content": user_prompt
            },
        ],
        temperature=0,
    )
    comment = response.choices[0].message.content
    text = f"This is a LLM-generated comment: \n{comment if comment else 'No feedback generated.'}"
    return text


def post_github_comment(git_token, repo, pr_number, comment):
    url = f"https://api.github.com/repos/{repo}/issues/{pr_number}/comments"
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {git_token}",
        "X-GitHub-Api-Version": "2022-11-28"
    }
    data = {"body": comment}
    response = requests.post(url, headers=headers, json=data)
    if response.status_code != 201:
        logger.error(
            f"Failed to create comment. Status code: {response.status_code} \n{response.text}"
        )
        raise Exception(
            f"Failed to create comment. Status code: {response.status_code} \n{response.text}"
        )
    logger.info(
        f"✅ Added review comment at https://github.com/{repo}/pull/{pr_number}"
    )


def main():
    submissions = get_submissions(submission_dir)
    if not submissions:
        logger.warning(
            f'No comments were generated because no files were found in the `{submission_dir}` directory. Please modify one or more of the files at `src/jobs/` or `src/tests/` to receive LLM-generated feedback.'
        )
        return None

    s3_solutions_dir = f"academy/2/homework-keys/{assignment}"
    local_solutions_dir = os.path.join(os.getcwd(), 'solutions', assignment)
    os.makedirs(local_solutions_dir, exist_ok=True)
    system_prompt, user_prompt = get_prompts(s3_solutions_dir,
                                             local_solutions_dir)

    for file_name, submission in submissions.items():
        file_path = os.path.join(submission_dir, file_name)
        user_prompt += code_snippet.format(file_name=file_name,
                                           file_content=submission)

    comment = get_feedback(system_prompt, user_prompt)
    if git_token and repo and pr_number:
        post_github_comment(git_token, repo, pr_number, comment)

    return comment


if __name__ == "__main__":
    comment = main()
    print(comment)
