"""Reddit comment data scraper."""

import pandas as pd
import requests
import time
import re

from pathlib import Path


class GetComments():
    """Class containing every function for obtaining reddit comment data."""

    def dict_formatter(variable: object,
                       input_text: str,
                       fail_return: object,
                       slice: int = 0):
        """
        Format data that is inserted into a Pandas DataFrame.

        Used to clean and format data that will be placed into a Pandas
        DataFrame by the comment_date() function.

        Args:
            variable: An object, primarily a dictionary obtained by using
                the requests .json() method. type: dict or None
            input_text: The dictionary key name that will be passed
                into the variable if there is a variable. type: str or None
            fail_return: The function returns this if the formatting failed.
                type: object
            slice: An integer representing where the variable key's value
                will be sliced if there is a variable. type: int

        Returns:
            String that will be the dictionary keys value.
        """
        invalid = ['[deleted]', '[removed]']
        if slice:
            return variable[input_text][slice:] if input_text in\
                variable.keys() and variable[input_text] not in\
                invalid else fail_return
        elif not slice and variable:
            return variable[input_text] if input_text in variable.keys()\
                and variable[input_text] not in invalid else fail_return
        elif not variable and not slice:
            return input_text if input_text not in invalid else fail_return

    def comment_data(request: dict) -> pd.core.frame.DataFrame:
        """
        Create a formatted list of dictionaries of comment data.

        Args:
            request: A dictionary obtained from the requests library
                using the .json() method. type: dict

        Returns:
            pd.DataFrame(comment_list), num_comments: A tuple of a
                Pandas DataFrame object and an integer representation
                for the number of comments retrieved.
                type: pandas.core.frame.DataFrame, int
        """
        num_comments = 0
        comment_list = []

        for request in request.get('data'):
            comment_body = request['body'].replace('\n', ' ')\
                                          .encode('ascii', 'ignore')\
                                          .decode()

            comment_body = re.sub(r' +', ' ', comment_body).strip()

            comment_list.append(
                {'comment_id': GetComments.dict_formatter(
                     request, 'id', None),
                 'submission_id': GetComments.dict_formatter(
                     request, 'link_id', None, 3),
                 'subreddit': GetComments.dict_formatter(
                     request, 'subreddit', None),
                 'subreddit_id': GetComments.dict_formatter(
                     request, 'subreddit_id', None, 3),
                 'author_id': GetComments.dict_formatter(
                     request, 'author_fullname', None, 3),
                 'author': GetComments.dict_formatter(
                     request, 'author', None),
                 'dt': GetComments.dict_formatter(
                     request, 'created_utc', 0),
                 'score': GetComments.dict_formatter(
                     request, 'score', None),
                 'awards_received': GetComments.dict_formatter(
                     request, 'total_awards_received', 0),
                 'body': GetComments.dict_formatter(
                     None, comment_body, None),
                 'parent_id': GetComments.dict_formatter(
                     request, 'parent_id', None, 3)})
            num_comments += 1

        return pd.DataFrame(comment_list), num_comments

    def get_comments(df: pd.core.frame.DataFrame) -> int:
        """
        Retrieve all comments from submissions in a Pandas DataFrame.

        Uses the pushshift API to retrieve every comment for a
        submission. The function retrieves 10000 comments at a time
        until there are no comments remaining for the submission.

        Args:
            df: A Pandas DataFrame containing an id, title, and date
                for every submisson that is pending comment retrieval.
                type: pandas.core.frame.DataFrame

        Returns:
            comment_counter: An integer representing how many comments
                were retrieved from pushshift. type: int
        """
        comment_counter = 0
        progress_count = 1
        df_length_str = str(len(df.index))

        for row in df.itertuples():
            print(f"({str(progress_count).zfill(len(df_length_str))}/"
                  f"{df_length_str}) Retrieving all "
                  f"comments for the following submission: '{row.title}'")
            url = 'https://api.pushshift.io/reddit/comment/search/'\
                f'?link_id={row.id}&limit=10000'

            while True:
                try:
                    request = requests.get(url).json()
                    break
                except Exception as e:
                    print(f'{e}\nRetrying in 10 seconds...')
                    time.sleep(10)

            comment_path = Path(f'./data/comment_data/{row.date.year}'
                                f'/{row.date.month}/{row.date.day}')
            file_number = 1

            if not comment_path.exists():
                comment_path.mkdir(parents=True)

            (comment_df1, num_comments) = GetComments.comment_data(request)

            comment_counter += num_comments

            comment_df1.to_csv(f'{comment_path}/{row.id}_comments_'
                               f'{str(file_number).zfill(2)}.csv',
                               index=None, quoting=2)

            while True:
                # Giving this some leniency in caseof an error during the
                # request.
                if len(comment_df1) > 8999:
                    before = comment_df1['dt'].min()
                    url = 'https://api.pushshift.io/reddit/comment/search/'\
                        f'?link_id={row.id}&before={before}&limit=10000'
                    while True:
                        try:
                            request = requests.get(url).json()
                            break
                        except Exception as e:
                            print(f'{e}\nRetrying in 10 seconds...')
                            time.sleep(10)

                    comment_df2, num_comments = GetComments\
                        .comment_data(request)

                    comment_counter += num_comments

                    if not comment_df2.equals(comment_df1):
                        if not comment_df2.empty:
                            file_number += 1
                            comment_df2.to_csv(
                                f'{comment_path}/{row.id}_comments_'
                                f'{str(file_number).zfill(2)}.csv',
                                index=None,
                                quoting=2
                            )
                            comment_df1 = comment_df2.copy()
                        else:
                            print('Skipping empty DataFrame...')
                            comment_df1 = comment_df2.copy()
                            pass
                    else:
                        # This entire block of code shouldn't ever be
                        # triggered.
                        print('Skipping duplicate DataFrame...')
                        comment_df1 = pd.DataFrame()
                        pass
                else:
                    break

            progress_count += 1

        return comment_counter
