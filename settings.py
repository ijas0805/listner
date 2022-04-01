import requests
import redis
import json, sys
import httpimport
# from 'https://github.com/firedrak/spider.git' import template

OUT_PUT_FILE_NAME = 'dataCollected.csv'

args = sys.argv[1:]
if args:
    redis_host = args[0]
    if args[2]:
        porcess_id = args[2]
        spider_url = args[1]
        with httpimport.remote_repo(["template"], spider_url):
            import template
    
else: spider_url = '' 

class redisCli:

    redis_host = redis_host
    redis_port = 6379

    REDIS_CLI = redis.StrictRedis(
        host=redis_host, port=redis_port, decode_responses=True)
    
    def heart_beat(self, porcess_id, spider_url):
        self.REDIS_CLI.set(f'heart_beat_of_{porcess_id}_{spider_url}', 'am_active!')
        self.REDIS_CLI.expire(f'heart_beat_of_{porcess_id}_{spider_url}', 3)

    def get_status(self, spider_url):
        return self.REDIS_CLI.get(f'state_of_{spider_url}')

    def start_crawling(self, spider_url):
        self.REDIS_CLI.set(f'state_of_{spider_url}', 'running')

    def stop_crawling(self, spider_url):
        self.REDIS_CLI.set(f'state_of_{spider_url}', 'stopped')

    def length_of_queue(self, key):
        return self.REDIS_CLI.llen(key)

    def redis_push(self, key, value):
        self.REDIS_CLI.lpush(key, json.dumps(value))

    def redis_pop(self, key):
        return json.loads(self.REDIS_CLI.rpop(key))

    def incr_process_count(self, porcess_id):
        self.REDIS_CLI.incr(f'process_of_{porcess_id}')

    def dicr_process_count(self, porcess_id):
        self.REDIS_CLI.decr(f'process_of_{porcess_id}')

    def get_process_count(self, porcess_id):
        return self.REDIS_CLI.get(f'process_of_{porcess_id}')
    
SPIDER_URL = spider_url
    
def first_job(spider_url):
    int_job = {'url' : template.STARTING_URL, 'call_back' : 'pars'}
    redisCli().redis_push(f'job_queue_of_{spider_url}', int_job)
    print(f'initial job added to redis for the spider {spider_url}')
