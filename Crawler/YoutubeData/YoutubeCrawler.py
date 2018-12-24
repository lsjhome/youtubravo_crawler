from googleapiclient.discovery import build
import multiprocessing as mp

class YoutubeParser(object):
    
    def __init__(self, api_key, processes=10):
        
        self.api_key = api_key
        self.client = build("youtube", "v3", developerKey=self.api_key)
        self.processes = processes        
        
    def _response(self, resource, **kwargs):
        
        '''
        Returns youtube client response depeonds on source

        Args:
            **kwargs: Arbitrary keyword arguments
            
        Returns:
            dict
            
        '''   
        kwargs = remove_empty_kwargs(**kwargs)
        
        if resource == 'channels':
            response = self.client.channels().list(
            **kwargs
            ).execute()
            
        if resource == 'search':
            response = self.client.search().list(
            **kwargs
            ).execute()
            
        if resource == 'videos':
            response = self.client.videos().list(
            **kwargs
            ).execute()
            
        if resource == 'playlistitems':
            response = self.client.playlistItems().list(
            **kwargs
            ).execute()
        
        return response
    
    def _split_list(self, l, n):

        '''
        yield n-sized list from list l

        args:
            l(list): origianl list to be split
            n(int): number of elements in the split list
        '''

        split_list = []

        for i in range(0, len(l), n):

            split_list.append(l[i:i + n]) 

        return split_list
    
        
    def channel_description(self, **kwargs):

        '''
        Returns Channel's introduction and its starting date

        Args:
            **kwargs: Arbitrary keyword arguments

        Returns:
            dictionary array: [{'title': channel_title,
                      chan          'ch_id': channel_id,
                                'description': channel_description,
                                'publisehdAt': channel_created_date}]

        '''
        
        responses = self._response('channels', **kwargs)         
        
        desc_date_list = [{'title': response['snippet']['title'],
                           'ch_id': response['id'],
                           'description':response['snippet']['description'],
                           'publishedAt':response['snippet']['publishedAt'][:10]} 
                            
                            for response in responses['items']]
        
        return desc_date_list
    
    def channel_statistics(self, **kwargs):
        
        '''
        Returns channel statistics
        
        Args:
            **kwargs: Arbitrary keyword arguments
            
        Returns:
            dictionary array: [{'ch_id': str,
                                'subscriberCount': int;None,
                                'viewCount': int,
                                'videoCount': int,
                                'sub_view_ratio': float;None}]
        '''
        
        responses = self._response('channels', **kwargs)        
        
        result_list = []
        
        for response in responses['items']:
            
            ch_id = response['id']
            
            statistics_response = response['statistics']
            
            
            if statistics_response['hiddenSubscriberCount'] is True:
                
                subscriber_count = None
                
                sub_view_ratio = None            
            
            else:
                subscriber_count = int(statistics_response['subscriberCount'])
                view_count = int(statistics_response['viewCount'])
                video_count = int(statistics_response['videoCount'])
                sub_view_ratio = view_count / subscriber_count                                   
                
            result_list.append({'ch_id': ch_id, 'subscriberCount':subscriber_count, 
                                'viewCount':view_count, 'videoCount':video_count, 
                                'sub_view_ratio':sub_view_ratio})  
        
        return result_list
    
    
    def channel_video_list(self, **kwargs):
        
        '''
        *** Calling multiple channels is not available ***
        
        Returns video id(s) by its channel(s)
        
        Args:
            **kwargs: Arbitrary keyword arguments
            
        Returns:
        
            dict: {'channelId': str, 'videoId': list}
        
        '''
        
        next_page_token = ''
        video_id_list = []
        
        while True:
            
            response = self._response('search', **kwargs, pageToken=next_page_token)
            
            video_ids = [item['id']['videoId'] for item in response['items']
                         if 'videoId' in item['id'].keys()]
            
            video_id_list.extend(video_ids)
                
            if 'nextPageToken' in response.keys():
                next_page_token = response['nextPageToken']

            else:
                
                return {'channelId' : kwargs['channelId'], 'videoId' : video_id_list}
            
    def video_statistics(self, **kwargs):
        
        '''
        Returns video statistics by its respective id
        
        Args:
            **kwargs: Arbitrary keyword arguments
            
        Returns:
            dictionary array: [{'videoId':id_video, 'statistics': video_statistics}, ..]      
        '''
        
        responses = self._response('videos', **kwargs)
        
        items = responses['items']
        
        video_statistics_list = []
        
        for item in items:
            
            id_video = item['id']
            video_statistics = item['statistics']
            video_statistics_list.append({'videoId' : id_video, 
                                          'statistics': video_statistics})        
        return video_statistics_list
    
    def _id_to_stats(self, x):
        
        id_join = ','.join(x)
        
        videos_stats = self.video_statistics(id=id_join, part='statistics')

        return videos_stats    
    
    def video_statistics_by_channel(self, **kwargs):
        
        response = self.channel_video_list(part='id', maxResults=50, **kwargs)
        
        video_id_list = response['videoId']
        
        video_split_list = self._split_list(video_id_list, 50)
        
        pool = mp.Pool(self.processes)
        
        results = [pool.apply_async(self._id_to_stats, args=([split])) for split in video_split_list]
        
        outputs = []
        
        for p in results:
            outputs.extend(p.get())
        
        return outputs

    def statistics_sum(self, *args):
        
        '''
        Returns the sum of values from video statistics dictionary array
        
        Args:
            *args: Arbitrary keyword arguments
            Dictionary array with key named 'statistics' and its value in dictionary
            
        Returns:
            
            dict: the sum of statistics
        
        '''

        vsc_sum = {}

        for vs in vsc:

            vs_stat = vs['statistics']
            keys = vs_stat.keys()

            for key in keys:

                if vs['statistics'][key].isdigit():

                    if key in vsc_sum.keys():

                        vsc_sum[key] += int(vs['statistics'][key])

                    else:
                        vsc_sum[key] = int(vs['statistics'][key])

        old_keys = [key for key in vsc_sum.keys()]

        for key in old_keys:
            vsc_sum[key + '_sum'] = vsc_sum[key]
            vsc_sum.pop(key)

        return vsc_sum
