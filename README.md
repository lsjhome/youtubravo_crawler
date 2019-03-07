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
  │  └─YoutubeCrawler.py
  │  └─__init__.py
  ├─YoutubeData
  │  └─__init__.py
  │  └─YoutubeCrawler.py
  ├─utils
  │  └─__init__.py
  │  └─mysql.py
  ├─comment_crawler.py # Thrid Crawler. Expected to run every three days
  ├─main_crawler.py    # Second Crawler. Expected to run every day
  └─trend_crawler.py   # First Crawler. Expected to run every hour
  ```

# Necessary enviroment variables
```
Google API
  API_1, API2, API3, API4, API5
DB
  host, db, user, port, pw 
```
