# Reddit Comment Scraper

---

## Project Background

This was originally written to scrape comment data for [Udacity's Data Engineering Nanodegree](https://www.udacity.com/course/data-engineer-nanodegree--nd027). I didn't end up using this to obtain data for my project but it works well nevertheless.

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

Note: All data will be saved in a newly created `./data` directory. Comment data will be categorized by `/{year}/{month}/{day}` directories.
