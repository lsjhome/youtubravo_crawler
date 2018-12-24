import logging
from datetime import datetime
import re

import requests
from bs4 import BeautifulSoup
from urllib3 import request
from fake_useragent import UserAgent

class MelonCrawler(object):
    
    def __init__(self, base_url = 'https://www.melon.com/chart/index.htm#params%5Bidx%'):
        
        self.base_url = base_url
        self.params = request.urlencode({'5D':1})
        
        
    def get_response(self, URL, params, headers):
    
        response = requests.get(URL, params, headers=headers)

        if response.status_code in [200, 201]:
            soup = BeautifulSoup(response.text, 'html.parser')
            return soup

        else:
            logging.error('get(%s) failed' % (URL, response.status_code))
            
            
    def _math_mark(self, dif):

        if dif == '순위 동일':
            return 0

        elif dif == '순위 진입':
            return 'NEW'

        number, mark = dif.split()
        number_pre = int(number[:-2])

        if mark == '하락':
            return -number_pre

        elif mark == '상승':
            return number_pre
        
    def run(self):
        
        base_tag = '#frm div table tbody tr'
        
        for param in self.params:
            
            ua = UserAgent()
            
            headers = {'User-Agent' : ua.ie}
            
            soups = self.get_response(self.base_url, param, headers=headers)
            
            soups_base = soups.select(base_tag)
            
            for soup in soups_base:
                
                rank = soup.select('span')[0].text
                dif = soup.select('span')[2].get('title')
                dif_pre = self._math_mark(dif)
                song = soup.select('span a')[0].text
                artists = soup.select('span a')[1:]
                artists = ', '.join([artist.text for artist in artists])
                created_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                print (rank, dif_pre, song, artists, created_time)


if __name__ == '__main__':
    
    mc = MelonCrawler()
    mc.run()
