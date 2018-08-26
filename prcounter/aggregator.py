"""GitHubからPullRequest, コメントを収集するためのモジュール"""
import sqlite3
from logging import getLogger

from github.GithubObject import NotSet

_logger = getLogger(__name__)


class GitHubAggregator(object):
    def __init__(self, db, gh_client):
        self.db = db
        self.gh_client = gh_client

    def aggregate(self, query):
        try:
            issues = self.gh_client.search_issues(query)
            for issue in issues:
                pull_request = issue.as_pull_request()
                if pull_request:
                    self.__aggregate_pull_request(pull_request)
                else:
                    self.__aggregate_issue(issue)
                self.db.commit()
        except sqlite3.IntegrityError as e:
            raise

    def __aggregate_issue(self, issue):
        # https://pygithub.readthedocs.io/en/latest/github_objects/Issue.html
        # issue commentを取得する
        raise NotImplementedError('まだissuesテーブルを定義していない')

    def __aggregate_pull_request(self, pull_request):
        _logger.debug(f'aggregate_pull_request() start [url={pull_request.url}, updated_at={pull_request.updated_at}]')
        # https://pygithub.readthedocs.io/en/latest/github_objects/PullRequest.html
        since = NotSet
        previous_updated_at = self.db.select_pull_request_updated_at(pull_request.url)
        _logger.debug(f'previous_updated_at = {previous_updated_at}')
        if previous_updated_at is None:
            pass
        elif previous_updated_at == pull_request.updated_at:
            # すでに処理済み
            # DBにはPR, コメントが格納されているので何もしない
            return
        elif previous_updated_at < pull_request.updated_at:
            since = previous_updated_at
        else:
            # DBが壊れている
            message = (
                f'updated_at in database is newer than updated_at returnd from GitHub API. '
                f'[url={pull_request.url},'
                f' github={pull_request.updated_at},'
                f' database={previous_updated_at}]')
            raise RuntimeError(message)

        # issue commentを取得する
        issue_comments = pull_request.get_issue_comments(since)
        for issue_comment in issue_comments:
            self.db.replace_issue_comment(issue_comment)

        # review を取得, 保存する
        reviews = pull_request.get_reviews()
        for review in reviews:
            self.db.replace_review(review)  # 重複する可能性がある

        # review commentを取得, 保存する
        review_comments = pull_request.get_review_comments(since)
        for review_comment in review_comments:
            self.db.replace_review_comment(review_comment)

        # PRを保存する
        self.db.replace_pull_request(pull_request)
        _logger.debug(f'aggregate_pull_request() finished [url={pull_request.url}]')
