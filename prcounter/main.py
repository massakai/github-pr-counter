import sys
from datetime import datetime
from logging import getLogger, StreamHandler, Formatter, DEBUG

import yaml
from github import Github
from github.GithubException import RateLimitExceededException

import prcounter.database
from prcounter.aggregator import GitHubAggregator


def setup_logger():
    formatter = Formatter('%(asctime)s %(levelname)s %(message)s')

    handler = StreamHandler()
    handler.setLevel(DEBUG)
    handler.setFormatter(formatter)
    logger = getLogger('prcounter')
    logger.setLevel(DEBUG)
    logger.addHandler(handler)


def main():
    setup_logger()

    conf_path = 'etc/config.yaml'

    with open(conf_path) as yaml_file:
        config = yaml.load(yaml_file)

    # TODO argparseで実行時に読み込む
    local = True
    startdate = '2018-01-01'
    enddate = '2018-03-31'
    users = config['github']['users']

    db = prcounter.database.Database(
        config['database']['path'],
        config['database']['sql_path'])
    db.connect()
    db.create_tables()

    # GitHubからデータを収集して, DBを更新する
    if not local:
        g = Github(
            base_url=config['github']['base_url'],
            login_or_token=config['github']['token'])

        aggregator = GitHubAggregator(db, g)

        try:
            for query in config['github']['search_issue_query']:
                aggregator.aggregate(query)
        except RateLimitExceededException as e:
            print(e.data['message'], file=sys.stderr)
            print(f'Documentation URL: {e.data["documentation_url"]}', file=sys.stderr)
        finally:
            requests_remaining, request_limit = g.rate_limiting
            rate_limiting_resettime = datetime.fromtimestamp(g.rate_limiting_resettime)
            print(f'GitHub Rate Limiting Info\n'
                  f'- requests remaining: {requests_remaining}\n'
                  f'- request_limit: {request_limit}\n'
                  f'- rate_limiting_resettime: {rate_limiting_resettime.isoformat()}',
                  file=sys.stderr)

    # 集計結果の表示
    result = db.count(startdate, enddate, users)

    db.close()

    # FIXME 動作確認
    from pprint import pprint
    pprint(result)

if __name__ == '__main__':
    main()
