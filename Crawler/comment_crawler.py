from datetime import datetime
import os

import pandas as pd

from utils.mysql import MySQL
from YoutubeData.YoutubeCrawler import YoutubeCrawler

def comment_pre(comment):
    """
    preprocess comment for DB INSERT
    """
    comment_list = list(comment)
    
    for comment_item in comment_list:
    
        if 'replyType' not in comment_item.keys():
            comment_item['replyType'] = False

        if type(comment_item['publishedAt']) == str and comment_item['publishedAt'].endswith('Z'):
            comment_item['publishedAt'] = comment_item['publishedAt'][:-1]
    
    return comment_list
    
def vid_id_list(day):
    """
    latest n video id from each channel for crawling comment
    """
    ch_vid_gen = conn.select('select ch_id, vid_id, vid_published_at from t_ch_vid_desc')
    ch_vid_list = list(ch_vid_gen)
    df = pd.DataFrame(ch_vid_list)
    df['rank'] = df.groupby('ch_id')['vid_published_at'].rank(ascending=False, method='first').values
    df_10 = df[df['rank'] <= day]
    vid_id_list = df_10['vid_id'].values
    
    return vid_id_list
    
def main():
        
    insert_vid_comment = 'INSERT IGNORE INTO YOUTUBE.t_vid_comment \
                    (published_at, n_like, aut_id, aut_name, aut_img_url, vid_comment, reply, vid_id) \
                    VALUES (%(publishedAt)s, %(likeCount)s, %(authorChannelId)s, %(authorDisplayName)s, \
                    %(authorProfileImageUrl)s, %(textDisplay)s, %(replyType)s, %(vid_id)s)'
    
    api_list = [os.environ['API_1'], os.environ['API_2'], os.environ['API_3'],
                os.environ['API_4'], os.environ['API_5']]
    
    conn = MySQL(host=os.environ['host'], 
             db=os.environ['db'], 
             user=os.environ['user'], 
             port=os.environ['port'], 
             passwd=os.environ['pw'],
             charset='utf8mb4',
             auto_commit=False)
    
    conn_02 = MySQL(host=os.environ['host'], 
             db=os.environ['db'], 
             user=os.environ['user'], 
             port=os.environ['port'], 
             passwd=os.environ['pw'],
             charset='utf8mb4',
             auto_commit=False)
        
    vid_id_list_return = vid_id_list(day=10)
    vid_id_list_join = ','.join(vid_id_list_return)
    yc = YoutubeCrawler(api_list, processes=50)
    comment = yc.comment(vids=vid_id_list_join)
    comment_list = comment_pre(comment)
    conn_02.connect()
    conn_02.executemany(insert_vid_comment, comment_list)
    conn_02.conn.commit()

if __name__ == '__main__':
    
    main()
