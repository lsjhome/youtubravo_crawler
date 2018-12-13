import logging
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from urllib3 import request
from fake_useragent import UserAgent

class NaverMusicCrawler(object):

    
    def __init__(self, base_url='https://music.naver.com/listen/top100.nhn?domain=TOTAL&duration=1h'):
        self.base_url = base_url
        self.params = [request.urlencode({'page' : i}) for i in range (1, 3)]
        

    def get_response(self, URL, params, headers):
    
        response = requests.get(URL, params, headers=headers)

        if response.status_code in [200, 201]:
            soup = BeautifulSoup(response.text, 'html.parser')
            return soup

        else:
            logging.error('get(%s) failed' % (URL, response.status_code))
            
            
    def _math_mark(self, diff):
    
        standard = diff[0]

        if standard == '상승':
            return int(diff[1])

        elif standard == '하락':
            return -int(diff[1])

        elif standard == '변동없음':
            return int(diff[1])

        elif standard == '신규':
            return 'NEW'
            
    def run(self):
        
        base_tag = 'div#content div div table tbody tr'
        
        for param in self.params:
            
            ua = UserAgent()
            
            headers = {'User-Agent' : ua.ie}
            
            soup = self.get_response(self.base_url, param, headers=headers)
            soup_select = soup.select(base_tag)[1:]
            
            for soup in soup_select:
                
                rank_now = soup.select('td.ranking')[0].text
                rank_dif = soup.select('td.change')[0].text.strip().split('\n')
                rank_dif_pre = self._math_mark(rank_dif)
                song = soup.select('td a span.ellipsis')[0].text.strip()
                artist = soup.select('td._artist.artist')[0].text.strip()
                created_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                print (rank_now, rank_dif_pre, song, artist, created_time)
