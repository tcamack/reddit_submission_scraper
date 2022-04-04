# Reddit Comment Scraper

---

## Project Background

This was originally written to scrape comment data for [my capstone project](https://github.com/tcamack/udacity-data-engineering-nanodegree/tree/master/05-capstone-project) for [Udacity's Data Engineering Nanodegree](https://www.udacity.com/course/data-engineer-nanodegree--nd027). I ended up changing the scope of the project and this went unused for its intended purpose.

## Project Information

This project uses the Reddit API via the PRAW Python library and the [pushshift API](https://pushshift.io/) to retrieve submission and comment data from a subreddit during a specific time period. It can retrieve all of the comments from *every* submission in a specified period of time or every comment for the *top* submissions in a specified period of time.

## How To Use

1. [Obtain a Reddit API Client ID and Secret Access Key](https://github.com/reddit-archive/reddit/wiki/OAuth2)
2. Input your Client ID and Secret Access Key into `./config/reddit.cfg`
3. Input your desired target settings into `main.py`
4. Run the following in your project directory terminal

```
pip install -r requirements.txt
```

5. Run the following in your project directory terminal

```
python main.py
```

Note: All data will be saved in a newly created `./data` directory. Comment data will be categorized by `/{year}/{month}/{day}` subdirectories.
