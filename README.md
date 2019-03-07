# Youtubravo Crawler

Data Crawler for [Youtubravo](http://www.youtubravo.com/bigdata.html)

```
Crawler
  ├─MusicChart
  │  └─Genie.py
  │  └─Melon.py
  │  └─Navermusic.py
  ├─MusicChart
  │  └─DaumKeyword.py
  │  └─NaverKeyword.py   
  ├─YoutubeData
  │  └─YoutubeCrawler.py # Youtube Data API Wrapper
  │  └─__init__.py
  ├─utils
  │  └─__init__.py
  │  └─mysql.py         # pymysql wrapper
  ├─comment_crawler.py  # Thrid Crawler for inserting youtube data into DB. Expected to run every three days
  ├─main_crawler.py     # Second Crawler for inserting youtube data into DB. Expected to run every day
  └─trend_crawler.py    # First Crawler for inserting youtube data into DB. Expected to run every hour
  ```

# Necessary enviroment variables
```
Google API
  - API_1, API2, API3, API4, API5
DB
  - host, db, user, port, pw 
```
