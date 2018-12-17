import logging
from datetime import datetime
import re

import requests
from bs4 import BeautifulSoup
from urllib3 import request
from fake_useragent import UserAgent

class DaumKeywordCrawler(object):
    
    def __init__(self, base_url='https://www.daum.net/', params=None):
        
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
        
        ua = UserAgent()
        
        headers = {'User-Agent': ua.ie}
        
        soup_org = self.get_response(self.base_url, self.params, headers)
        base_tag = 'div.hotissue_builtin'
        soup_base = soup_org.select(base_tag)
        ranks = soup_base[0].select('span.ir_wa')[::2]
        keywords = soup_base[0].select('a.link_issue')[::2]
        created_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        rank_keyword_time = [(rank.text, keyword.text, created_time) for (rank, keyword) in zip(ranks, keywords)]
        
        print (rank_keyword_time)
        
if __name__ =='__main__':
    
    dk = DaumKeywordCrawler()
    dk.run()
