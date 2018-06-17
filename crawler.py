import json
import os
from collections import deque
from re import findall
from time import time, sleep
from util import randselect, byteify, file_to_list
import pprint # test log
import datetime # to convet timestamp to ISODate
import multiprocessing as mp #for multiprocessing
from pymongo import MongoClient
import csv

def crawl(api, hashtag, config):
    # print('Crawling started at origin hashtag', origin['user']['username'], 'with ID', origin['user']['pk'])
     while True:
        feed = get_posts(api, hashtag, config)

    #if visit_profile(api, hashtag, config):
    #    pass

def upload_mongo(api, feed, config):
    print("upload_mongo")
    processed_tagfeed = {
        'posts' : []
    }
    profile_dic = {}
    posts = [beautify_post(api, post, profile_dic) for post in feed]
    posts = list(filter(lambda x: not x is None, posts))
    if len(posts) < config['min_collect_media']:
        return False
    else:
        processed_tagfeed['posts'] = posts[:config['max_collect_media']]
    client = MongoClient('localhost', 27017)
    db = client.BigData

    pp = pprint.PrettyPrinter(indent=2)
    print ("Start upload")
    for post in processed_tagfeed['posts']:
        #print(db.post.find({"user_id": post['user_id'], "date": post['date']}).limit(1).count())
        print(post['date'])
        if db.post.find({"user_id": post['user_id'], "date": post['date']}).limit(1).count() < 1:
            db.post.insert(post) 
        else:
            pass

def beautify_post(api, post, profile_dic):
    #print("start beautify_post")
    try:
        if post['media_type'] != 1: # If post is not a single image media
            return None
        keys = post.keys()
        user_id = post['user']['pk']
        processed_media = {
            'user_id' : user_id,
            #'username' : profile['user']['username'],
            #'full_name' : profile['user']['full_name'],
            #'profile_pic_url' : profile['user']['profile_pic_url'],
            #'media_count' : profile['user']['media_count'],
            #'follower_count' : profile['user']['follower_count'],
            #'following_count' : profile['user']['following_count'],
            'date' : datetime.datetime.fromtimestamp(post['taken_at']).isoformat(),
            #'pic_url' : post['image_versions2']['candidates'][0]['url'],
            #'like_count' : post['like_count'] if 'like_count' in keys else 0,
            #'comment_count' : post['comment_count'] if 'comment_count' in keys else 0,
            'caption' : post['caption']['text'] if 'caption' in keys and post['caption'] is not None else ''
        }
        processed_media['tags'] = findall(r'#[^#\s]*', processed_media['caption'])
        # print(processed_media['tags'])
        return processed_media
    except Exception as e:
        print('exception in beautify post')
        raise e

def get_posts(api, hashtag, config):
    print("start_get_posts")
    try:
        feed_len = 0
        feed = []
        pp = pprint.PrettyPrinter(indent=2)
        #pp.pprint(config)
        try:
            uuid = api.generate_uuid(return_hex=False, seed='0')
            #results = api.feed_tag(hashtag, rank_token=uuid, min_timestamp=config['min_timestamp'])
            results = api.feed_tag(hashtag, rank_token=uuid, max_id=config['max_id'])
        except Exception as e:
            print('exception while getting feed1')
            raise e
        feed_len = 0
        feed_len += len(results.get('items', []))
        if config['min_timestamp'] is not None :
            upload_mongo(api, results.get('items', []), config)

        client = MongoClient()
        db = client.BigData
        print('multi')
        jobs = []
        next_max_id = results.get('next_max_id')
        while next_max_id and len(feed) < config['max_collect_media']:
            db.config.update_one({},{'$set': {'max': next_max_id}}, upsert=False)
            #print("next_max_id:", next_max_id)
            print("len(feed):", feed_len, "< max:", config['max_collect_media'])
            try:
                results = api.feed_tag(hashtag, rank_token=uuid, max_id=next_max_id)
            except Exception as e:
                print('exception while getting feed2')
                if str(e) == 'Bad Request: Please wait a few minutes before you try again.':
                    sleep(60)
                else:
                    raise e
            feed_len += len(results.get('items', []))
            #feed.extend(results.get('items', []))
            next_max_id = results.get('next_max_id')
            #print(next_max_id)
            #db.config.update_one({},{'$set': {'max': next_max_id}}, upsert=False)
            #upload_mongo(api, results.get('items', []), config, db)
            p = mp.Process(target=upload_mongo, args=(api, results.get('items', []), config))
            jobs.append(p)
            p.start()
        for p in jobs:
            p.join()
        client.close()
    except Exception as e:
        print('exception while getting posts')
        raise e


