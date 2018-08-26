import sqlite3
from logging import getLogger

import yaml

_logger = getLogger(__name__)


class Database(object):
    def __init__(self, path, sql_path):
        self.path = path
        with open(sql_path) as sql_file:
            self.__table_sql = yaml.load(sql_file)
        self.__conn = None

    def connect(self):
        self.__conn = sqlite3.connect(
            self.path, detect_types=sqlite3.PARSE_DECLTYPES)

    def create_tables(self):
        create_statements = [
            (name, table['create'])
            for name, table in self.__table_sql.items()]
        for name, stmt in create_statements:
            _logger.debug(f'creating table "{name}"')
            cursor = self.__conn.cursor()
            cursor.execute(stmt)

    def commit(self):
        self.__conn.commit()

    def close(self):
        self.__conn.close()

    def replace_pull_request(self, pull_request):
        params = (
            pull_request.url,
            pull_request.user.login,
            pull_request.created_at,
            pull_request.updated_at,
            pull_request.merged_at,
            pull_request.issue_url,
        )
        cursor = self.__conn.cursor()
        cursor.execute(self.__table_sql['pull_requests']['replace'], params)

    def select_pull_request_updated_at(self, url):
        params = (url, )
        cursor = self.__conn.cursor()
        cursor.execute(self.__table_sql['pull_requests']['select_updated_at'], params)
        row = cursor.fetchone()
        if row:
            return row[1]

    def replace_issue_comment(self, issue_comment):
        params = (
            issue_comment.url,
            issue_comment.user.login,
            issue_comment.created_at,
            issue_comment.issue_url,
        )
        cursor = self.__conn.cursor()
        cursor.execute(self.__table_sql['issue_comments']['replace'], params)


    def replace_review(self, review):
        _logger.debug(f'updating review [review.id = {review.id}, review.pull_request_url = {review.pull_request_url}]')
        # GitHub APIがURLを返さないので作成する
        url = f'{review.pull_request_url}/reviews/{review.id}'
        params = (
            url,
            review.user.login,
            review.submitted_at,
            review.pull_request_url,
        )
        cursor = self.__conn.cursor()
        cursor.execute(self.__table_sql['pull_request_reviews']['replace'], params)


    def replace_review_comment(self, review_comment):
        params = (
            review_comment.url,
            review_comment.user.login,
            review_comment.created_at,
            review_comment.pull_request_url,
        )
        cursor = self.__conn.cursor()
        cursor.execute(self.__table_sql['pull_request_comments']['replace'], params)

    def count(self, startdate, enddate, users=None):
        query = (
            "SELECT "
            "  user, "
            "  sum(created_pull_request_count) AS created_pull_request_count, "
            "  sum(merged_pull_request_count) AS merged_pull_request_count, "
            "  sum(commented_pull_request_count) AS commented_pull_request_count, "
            "  sum(comment_count) AS comment_count "
            "FROM "
            "  all_counts "
            "WHERE "
            "  date BETWEEN ? AND ? "
            "GROUP BY "
            "  user")
        cursor = self.__conn.cursor()
        cursor.execute(query, (startdate, enddate))

        keys = ('user',
                'created_pull_request_count',
                'merged_pull_request_count',
                'commented_pull_request_count',
                'comment_count')
        return [dict(zip(keys, row))
                for row in cursor.fetchall()
                if users is None or row[0] in users]