import argparse
import sys
from datetime import datetime
from logging import getLogger, NullHandler, FileHandler, Formatter, DEBUG

import yaml
from github import Github
from github.GithubException import RateLimitExceededException

import prcounter.database
from prcounter.aggregator import GitHubAggregator


def setup_logger(path=None):
    handler = NullHandler()
    if path is not None:
        formatter = Formatter('%(asctime)s %(levelname)s %(message)s')
        handler = FileHandler(path)
        handler.setLevel(DEBUG)
        handler.setFormatter(formatter)
    logger = getLogger('prcounter')
    logger.setLevel(DEBUG)
    logger.addHandler(handler)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument('--config', '-c',
                        default='etc/config.yaml',
                        help='設定ファイルのパス')
    parser.add_argument('--local',
                        action = 'store_true',
                        help='GitHubからデータの収集を行わず、ローカルDBを使用する')
    parser.add_argument('--startdate', '-s',
                        default='2018-01-01',  # FIXME
                        help='集計開始日 YYYY-MM-DD')
    parser.add_argument('--enddate', '-e',
                        default='2018-03-31',  # FIXME
                        help='集計終了日 YYYY-MM-DD')

    args = parser.parse_args()

    with open(args.config) as yaml_file:
        config = yaml.load(yaml_file)

    setup_logger(config['application']['log_path'])

    # バリデーション
    startdate = datetime.strptime(args.startdate, '%Y-%m-%d')
    enddate = datetime.strptime(args.enddate, '%Y-%m-%d')
    assert startdate <= enddate

    users = config['github']['users']

    db = prcounter.database.Database(
        config['database']['path'],
        config['database']['sql_path'])
    db.connect()
    db.create_tables()

    # GitHubからデータを収集して, DBを更新する
    if not args.local:
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
    result = db.count(args.startdate, args.enddate, users)

    db.close()

    # FIXME 動作確認
    from pprint import pprint
    pprint(result)

if __name__ == '__main__':
    main()
