import os

from utils.mysql import MySQL
from YoutubeData.YoutubeCrawler import YoutubeCrawler

api_list = [os.environ['API_1'], os.environ['API_2'], os.environ['API_3'],
            os.environ['API_4'], os.environ['API_5']]
            
conn = MySQL(host=os.environ['host'], 
             db=os.environ['db'], 
             user=os.environ['user'], 
             port=os.environ['port'], 
             passwd=os.environ['pw'],
             charset='utf8mb4',
             auto_commit=False)
             
def th_url_join(video_list, thumb, t):
    
    for video in video_list:
        th_list = []
        for key in video[thumb].keys():
            th_list.append(video[thumb][key]['url'])
        video[thumb] = ', '.join(th_list)
        
        video[t] = video[t][:-1]
        
    return video_list
    
def main():

    yc = YoutubeCrawler(api_list, processes=50)
    vmp_list = yc.video_trend(rc='KR', top=True)
    vmp_list_pre = th_url_join(vmp_list, 'vid_th', 'vid_published_at')

    insert_vid_trend = 'INSERT IGNORE INTO YOUTUBE.t_vid_trend \
                       (vid_id, vid_published_at, ch_id, vid_title, vid_desc, vid_th, ch_title, \
                       vid_tags, vid_cat_id, region_code, vid_cat, vid_rank, vid_trend_cat) \
                       VALUES (%(vid_id)s, %(vid_published_at)s, %(ch_id)s, %(vid_title)s, \
                       %(vid_desc)s, %(vid_th)s, %(ch_title)s, %(vid_tags)s, %(vid_cat_id)s, \
                       %(region_code)s, %(vid_cat)s, %(vid_rank)s, %(vid_trend_cat)s) \
                       '
    conn.executemany(insert_vid_trend, vmp_list_pre)
    conn.conn.commit()
    
if __name__ == '__main__':
    
    main()
