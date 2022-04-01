
from settings import *

redisClient = redisCli()
if redisClient.get_status(SPIDER_URL) != 'running':
    redisClient.start_crawling(SPIDER_URL)
    first_job(SPIDER_URL)

import asyncio
import template
import aiohttp
from bs4 import BeautifulSoup
from multiprocessing import Process

redisClient = redisCli()

procersses = []

conn = aiohttp.TCPConnector(limit_per_host=100, limit=0, ttl_dns_cache=300)

async def extracting():
# Processing pages in redis queue for data and urls.

    def process_page(page):
        doc = BeautifulSoup(page['content'], 'html.parser')
        fun_to_call = getattr(template, page['call_back'])
        result = fun_to_call(doc)
        if 'data' in result.keys() and len(result['data']):
            redisClient.redis_push(f'data_of_{SPIDER_URL}', result['data'])
        if 'url' in result.keys() and len(result['url']):
            for job in result['url']:
                redisClient.redis_push(f'job_queue_of_{SPIDER_URL}', job)

    while redisClient.get_status(SPIDER_URL) == 'running':
        redisClient.heart_beat(porcess_id, spider_url)
        page_left = (redisClient.length_of_queue(f'page_queue_of_{SPIDER_URL}'))
        if page_left:
            redisClient.incr_process_count(porcess_id)
            try:
                page = redisClient.redis_pop(f'page_queue_of_{SPIDER_URL}')
            except:
                page = None
            if page:
                process_page(page)
            redisClient.dicr_process_count(porcess_id)
        await asyncio.sleep(.1)

async def fetching():
# Downloading pages and pushing it to redis queue

    semaphore = asyncio.Semaphore(50)
    session = aiohttp.ClientSession(connector=conn)
    
    async def push_page(job):
        url = job['url']
        try:
            async with semaphore:
                async with session.get(url, ssl=False) as response:
                    doc = await response.text()
                    redisClient.redis_push(f'page_queue_of_{SPIDER_URL}', {'content':doc, 'call_back':job['call_back']})
        except: print('404')
        redisClient.dicr_process_count(porcess_id)
            
    while redisClient.get_status(SPIDER_URL) == 'running':
        redisClient.heart_beat(porcess_id, spider_url)
        job_left = (redisClient.length_of_queue(f'job_queue_of_{SPIDER_URL}'))
        if job_left:
            redisClient.incr_process_count(porcess_id)
            asyncio.create_task(push_page(redisClient.redis_pop(f'job_queue_of_{SPIDER_URL}')))
        await asyncio.sleep(1)

    await session.close()

async def main():
    
    fetching_routine = asyncio.ensure_future(fetching())
    extracting_routine = asyncio.ensure_future(extracting())
    
    while redisClient.get_status(SPIDER_URL) == 'running':

        await asyncio.sleep(1)
        if redisClient.length_of_queue(f'job_queue_of_{SPIDER_URL}') == 0 and redisClient.length_of_queue(f'page_queue_of_{SPIDER_URL}') == 0:
            await asyncio.sleep(.5)
            if int(redisClient.get_process_count(porcess_id)) < 1:
                redisClient.stop_crawling(SPIDER_URL)

        job_len = redisClient.length_of_queue(f'job_queue_of_{SPIDER_URL}')
        page_len = redisClient.length_of_queue(f'page_queue_of_{SPIDER_URL}')
        data_len = redisClient.length_of_queue(f'data_of_{SPIDER_URL}')
        state = redisClient.get_status(SPIDER_URL)
        count = redisClient.get_process_count(porcess_id)
        print(f'job_len:{job_len} page_len:{page_len} count:{count} data_len:{data_len} state:{state}')

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())



    # import pdb; pdb.set_trace()
