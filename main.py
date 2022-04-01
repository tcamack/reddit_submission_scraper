"""Program to obtain submission and comment data from Reddit."""

import configparser

from datetime import datetime
from modules import GetSubmissions, GetComments


def main(start_date: datetime,
         end_date: datetime,
         subreddit_name: str,
         search_term: str,
         sort_type: str,
         results_per_day: int,
         client_id: str,
         secret_key: str) -> int:
    """
    Begin and manage the submission and comment retrieval process.

    Args:
        start_date: A datetime.datetime instance to indicate when to
            begin retrieving information. type: datetime.datetime
        end_date: A datetime.datetime instance to indicate when to
            stop retrieving information. Must be later than the start
            date. type: datetime.datetime
        subreddit_name: The specified subreddit name as a string. The
            string should not include 'r/' before the name. type: str
        search_term: A specific term string to search for in the specified
            subreddits submission titles. type: str
        sort_type: The parameter which pushshift submission information is
            sorted. Defaults to 'num_comments' as pushshift doesn't accurately
            track submission score. type: str
        results_per_day: Integer to limit daily results obtained from the
            pushshift API. type: int
        client_id: Client ID obtained from reddit for API access. Stored in
            an external file './config/reddit.cfg'. type: str
        secret_key: Secret key obtained form reddit for API access. Stored in
            an external file './config/reddit.cfg'. type: str

    Returns:
        comment_counter: An integer representing how many comments were
            retrieved from pushshift. type: int

    Reddit API documentation:
        https://www.reddit.com/dev/api/
    Pushshift API documentation:
        https://pushshift.io/api-parameters/
    """
    term = f"containing '{search_term}' in the title " if search_term else ""

    print(f"Retrieving the most active '{subreddit_name}' submissions {term}"
          f"between {start_date} and {end_date}...")

    submission_df = GetSubmissions\
        .get_top_submissions(start=start_date,
                             end=end_date,
                             subreddit=subreddit_name,
                             results_per_day=results_per_day,
                             search_term=search_term,
                             sort_type=sort_type,
                             client_id=client_id,
                             secret_key=secret_key)

    comment_counter = GetComments.get_comments(df=submission_df)

    print('\nDone! All available data has been successfully saved locally.')

    return comment_counter


if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('./config/reddit.cfg')

    client_id = config['API_CREDENTIALS']['client_id']
    secret_key = config['API_CREDENTIALS']['secret_key']
    subreddit_name = ''
    search_term = ''
    sort_type = 'num_comments'
    results_per_day = 5

    start_date = datetime(2021, 1, 1)
    end_date = datetime(2021, 2, 1)

    if start_date < end_date:
        start = datetime.now()
        comment_counter = main(start_date=start_date,
                               end_date=end_date,
                               subreddit_name=subreddit_name,
                               search_term=search_term,
                               sort_type=sort_type,
                               results_per_day=results_per_day,
                               client_id=client_id,
                               secret_key=secret_key)

        print(f'\nData retrieval time: {datetime.now() - start}')
        print(f'\nTotal comments retrieved: {comment_counter}')
    else:
        print('The end date must be after the start date.')
