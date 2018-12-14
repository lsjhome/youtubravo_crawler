import logging
from datetime import datetime
import re

import requests
from bs4 import BeautifulSoup
from urllib3 import request
from fake_useragent import UserAgent

class NaverKeywordCraler(object):
    
    def __init__(self, base_url='https://datalab.naver.com/keyword/realtimeList.naver', params=None):
        
        self.base_url = base_url
        self.params = params
        
        
    def get_response(self, URL, params, headers):
    
        response = requests.get(URL, params, headers=headers)

        if response.status_code in [200, 201]:
            soup = BeautifulSoup(response.text, 'html.parser')
            return soup

        else:
            logging.error('get(%s) failed' % (URL, response.status_code))
            
    def run(self):
        
        base_tag = '#content div div div div div div.rank_inner'
        
        ua = UserAgent()
        
        headers = {'User-Agent': ua.ie}
        
        soups_org = self.get_response(self.base_url, self.params, headers)
        
        soups_base = soups_org.select(base_tag)
        
        for soups in soups_base:

            category = soups.select('strong')[0].text
            soups_tag = soups.select('div ul a')
            for soup in soups_tag:
                rank = int(soup.select('em')[0].text)
                keyword = soup.select('span')[0].text
                created_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                print (category, rank, keyword, created_time)


if __name__ == '__main__':
    
    nc = NaverKeywordCraler()
    nc.run()
