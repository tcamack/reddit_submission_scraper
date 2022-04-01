"""Reddit submission data scraper."""

import pandas as pd
import requests
import time
import praw
import progressbar

from datetime import datetime, timezone
from pathlib import Path


class GetSubmissions():
    """Class containing every function for obtaining reddit submission data."""

    def get_updated_submission_information(df: pd.core.frame.DataFrame,
                                           client_id: str,
                                           secret_key: str) -> None:
        """
        Retrieve updated submission information from the official Reddit API.

        Pushshift stores the submission score from the time the submission was
        first scraped by the API. This function queries the official Reddit API
        to obtain the most recent submission information. After obtaining
        updated information from the Reddit API the dataframe is saved to an
        external json file.

        Args:
            df: A Pandas DataFrame containing submission information obtained
                from the pushshift API. type. pandas.core.frame.DataFrame
            client_id: Client ID obtained from reddit for API access. Stored
                in an external file './config/reddit.cfg'. type: str
            secret_key: Secret key obtained form reddit for API access. Stored
                in an external file './config/reddit.cfg'. type: str

        Returns:
            None
        """
        print('Finalizing submission data...')
        reddit = praw.Reddit(
            client_id=client_id,
            client_secret=secret_key,
            user_agent='SubmissionDataScraper/0.0.1'
        )

        submission_information = []

        while True:
            try:
                for row in df.itertuples():
                    sub = reddit.submission(row.id)

                    submission_information.append(
                        {'id': sub.id,
                         'score': sub.score,
                         'upvote_ratio': sub.upvote_ratio})
                break
            except Exception as e:
                print(f'{e}\nRetrying in 10 seconds...')
                time.sleep(10)

        supplemental = pd.DataFrame(submission_information)

        df['score'] = supplemental['score']
        df['upvote_ratio'] = supplemental['upvote_ratio']

        submission_data_path = Path(f'./data')

        if not submission_data_path.exists():
            submission_data_path.mkdir(parents=True)

        df = df[['created_utc', 'id', 'title', 'author_fullname',
                 'author', 'num_comments', 'full_link',
                 'selftext', 'score', 'upvote_ratio']]

        df.to_json(f'{submission_data_path}/submission_data.json',
                   orient='records', indent=4)

    def get_all_submissions(start: datetime,
                            end: datetime,
                            subreddit: str,
                            client_id: str,
                            secret_key: str,
                            search_term: str = '') -> pd.core.frame.DataFrame:
        """
        Retrieve all submission data from the pushshift API in a timeframe.

        This method is significantly slower but does allow for more accuracy
        when parsing submission data. I would recommend only using this for
        less active subreddits or if you have time blocked out for retrieving
        data.

        Args:
            start: A datetime.datetime instance to indicate when to
                begin retrieving information. type: datetime.datetime
            end: A datetime.datetime instance to indicate when to
                stop retrieving information. Must be later than the
                start date. type: datetime.datetime
            subreddit: The specified subreddit name as a string. The
                string should not include 'r/' before the name. type: str
            client_id: Client ID obtained from reddit for API access. Stored
                in an external file './config/reddit.cfg'. type: str
            secret_key: Secret key obtained form reddit for API access. Stored
                in an external file './config/reddit.cfg'. type: str
            search_term: A specific term string to search for in the specified
                subreddits submission titles. type: str

        Returns:
            df: A Pandas DataFrame containing information needed for comment
                retrieval. type: pandas.core.frame.DataFrame
        """
        start_timestamp = int(start.replace(tzinfo=timezone.utc).timestamp())
        end_timestamp = int(end.replace(tzinfo=timezone.utc).timestamp())
        progress_start = start_timestamp
        progress_end = end_timestamp - start_timestamp
        widgets = [progressbar.GranularBar(
                   progressbar.widgets.GranularMarkers.smooth),
                   progressbar.Percentage(format='%(percentage)3d%% '),
                   progressbar.AdaptiveETA()]
        bar = progressbar.ProgressBar(max_value=progress_end,
                                      widgets=widgets)
        df = pd.DataFrame()
        search_term = f'q={search_term}&' if search_term else ''
        bar.start()

        while True:
            try:
                while start_timestamp <= end_timestamp:
                    url = f'https://api.pushshift.io/reddit/search/'\
                          f'submission/?{search_term}subreddit={subreddit}'\
                          f'&after={start_timestamp}&before={end_timestamp}'\
                           '&limit=100'

                    data = requests.get(url).json()

                    temp_df = pd.DataFrame(data['data'])

                    if data['data']:
                        df = pd.concat([df, temp_df])
                    else:
                        break

                    start_timestamp = df['created_utc'].max()

                    bar.update(start_timestamp - progress_start)

                    time.sleep(1)
                bar.finish()
                break
            except Exception as e:
                print(f'\n{e}\nRetrying in 10 seconds...')
                time.sleep(10)

        df = df[['created_utc', 'id', 'title', 'author_fullname',
                'author', 'num_comments', 'full_link', 'selftext']]

        df['selftext'] = df['selftext'].str.replace('\n', ' ', regex=False)\
                                       .str.encode('ascii', 'ignore')\
                                       .str.decode('ascii')\
                                       .str.replace(r' +', ' ', regex=True)\
                                       .str.replace('[deleted]', '',
                                                    regex=False)\
                                       .str.replace('[removed]', '',
                                                    regex=False)\
                                       .str.strip()

        df['title'] = df['title'].str.replace('\n', ' ', regex=False)\
                                 .str.encode('ascii', 'ignore')\
                                 .str.decode('ascii')\
                                 .str.replace(r' +', ' ', regex=True)\
                                 .str.replace('[deleted]', '', regex=False)\
                                 .str.replace('[removed]', '', regex=False)\
                                 .str.strip()

        df['author_fullname'] = df['author_fullname'].str.slice(3)

        df['author'] = df['author'].replace('[deleted]', '', regex=False)\
                                   .replace('[removed]', '', regex=False)

        df['date'] = pd.to_datetime(df['created_utc'], unit='s')

        GetSubmissions.get_updated_submission_information(df, client_id,
                                                          secret_key)

        return df[['id', 'title', 'date']]

    def get_top_submissions(start: datetime,
                            end: datetime,
                            subreddit: str,
                            client_id: str,
                            secret_key: str,
                            sort_type: str = 'num_comments',
                            results_per_day: int = 5,
                            search_term: str = '') -> pd.core.frame.DataFrame:
        """
        Retrieve top 'n' posts by 'sort_type' in a timeframe.

        Recommended over retrieving all submissions from a specified timeframe
        due to time required to retreive information. The Reddit API and
        pushshift API both have rate limits that make this a significantly
        faster process if you know the exact information you are looking for.

        Args:
            start: A datetime.datetime instance to indicate when to
                begin retrieving information. type: datetime.datetime
            end: A datetime.datetime instance to indicate when to
                stop retrieving information. Must be later than the
                start date. type: datetime.datetime
            subreddit: The specified subreddit name as a string. The
                string should not include 'r/' before the name. type: str
            client_id: Client ID obtained from reddit for API access. Stored
                in an external file './config/reddit.cfg'. type: str
            secret_key: Secret key obtained form reddit for API access. Stored
                in an external file './config/reddit.cfg'. type: str
            sort_type: The parameter which pushshift submission information is
                sorted. Defaults to 'num_comments' as pushshift doesn't
                accurately track submission score. type: str
            results_per_day: Integer to limit daily results obtained from the
                pushshift API. type: int
            search_term: A specific term string to search for in the specified
                subreddits submission titles. type: str

        Returns:
            df: A Pandas DataFrame containing information needed for comment
                retrieval. type: pandas.core.frame.DataFrame
        """
        start_timestamp = int(start.replace(tzinfo=timezone.utc).timestamp())
        end_timestamp = int(end.replace(tzinfo=timezone.utc).timestamp())
        progress_end = end_timestamp - start_timestamp
        widgets = [progressbar.GranularBar(
                   progressbar.widgets.GranularMarkers.smooth),
                   progressbar.Percentage(format='%(percentage)3d%% '),
                   progressbar.AdaptiveETA()]
        bar = progressbar.ProgressBar(max_value=progress_end,
                                      widgets=widgets)
        df = pd.DataFrame()
        search_term = f'q={search_term}&' if search_term else ''
        bar.start()

        for date in pd.date_range(start, end, inclusive='left'):
            start_utc = int(date.replace(tzinfo=timezone.utc).timestamp())
            end_utc = start_utc + 86400

            while True:
                try:
                    url = f'https://api.pushshift.io/reddit/search/'\
                            f'submission/?{search_term}subreddit={subreddit}'\
                            f'&after={start_utc}&before={end_utc}'\
                            f'&sort_type={sort_type}&sort=desc'\
                            f'&limit={results_per_day}'

                    data = requests.get(url).json()

                    temp_df = pd.DataFrame(data['data'])

                    if data['data']:
                        df = pd.concat([df, temp_df])
                    else:
                        break

                    bar.update(end_utc - start_timestamp)

                    time.sleep(1)
                    break
                except Exception as e:
                    print(f'\n{e}\nRetrying in 10 seconds...')
                    time.sleep(10)

        bar.finish()

        df = df[['created_utc', 'id', 'title', 'author_fullname',
                'author', 'num_comments', 'full_link', 'selftext']]

        df['selftext'] = df['selftext'].str.replace('\n', ' ', regex=False)\
                                       .str.encode('ascii', 'ignore')\
                                       .str.decode('ascii')\
                                       .str.replace(r' +', ' ', regex=True)\
                                       .str.replace('[deleted]', '',
                                                    regex=False)\
                                       .str.replace('[removed]', '',
                                                    regex=False)\
                                       .str.strip()

        df['title'] = df['title'].str.replace('\n', ' ', regex=False)\
                                 .str.encode('ascii', 'ignore')\
                                 .str.decode('ascii')\
                                 .str.replace(r' +', ' ', regex=True)\
                                 .str.replace('[deleted]', '', regex=False)\
                                 .str.replace('[removed]', '', regex=False)\
                                 .str.strip()

        df['author_fullname'] = df['author_fullname'].str.slice(3)

        df['author'] = df['author'].replace('[deleted]', '', regex=False)\
                                   .replace('[removed]', '', regex=False)

        df['date'] = pd.to_datetime(df['created_utc'], unit='s')

        GetSubmissions.get_updated_submission_information(df, client_id,
                                                          secret_key)

        return df[['id', 'title', 'date']]
