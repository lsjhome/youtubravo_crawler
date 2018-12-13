import logging
from datetime import datetime
import re

import requests
from bs4 import BeautifulSoup
from urllib3 import request
from fake_useragent import UserAgent

class GenieCrawler(object):

    
    def __init__(self, base_url='https://www.genie.co.kr/chart/top200?'):

        self.base_url = base_url
        self.params = [request.urlencode({'pg' : i}) for i in range (1, 5)]
        

    def get_response(self, URL, params, headers):
    
        response = requests.get(URL, params, headers=headers)

        if response.status_code in [200, 201]:
            soup = BeautifulSoup(response.text, 'html.parser')
            return soup

        else:
            logging.error('get(%s) failed' % (URL, response.status_code))
            
            
    def _math_mark(self, dif):

        found = re.findall('\d+', dif)

        if len(found) == 0:

            if dif == '유지':

                return 0

            elif dif == 'new':

                return 'new'

        else:

            if '상승' in dif:
                
                return int(found[0])            
            
            elif '하강' in dif:
            
                return -int(found[0])
            
                        
    def run(self):
        
        base_tag = '#body-content div div table tbody tr'
        
        for param in self.params:
            
            ua = UserAgent()
            
            headers = {'User-Agent': ua.ie}
            
            soups = self.get_response(self.base_url, param, headers=headers)
            
            soups_base = soups.select(base_tag)
            
            for soup in soups_base:
                
                rank_and_dif = soup.select('td.number')
                rank_now, dif = re.sub(r'\s', ' ', rank_and_dif[0].text).split()
                dif_pre = self._math_mark(dif)
                song = soup.select('td a.title.ellipsis')[0].text.strip()
                artist = soup.select('td a.artist.ellipsis')[0].text.strip()
                created_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                print (rank_now, dif_pre, song, artist, created_time)     
