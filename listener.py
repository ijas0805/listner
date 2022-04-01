import redis
import os, sys, time
import subprocess
import socket
from multiprocessing import Process

# Sample command to use listener.py
# python3 listener.py redis_host max_processes listener_name
args = sys.argv[1:]
if args:
    redis_host = args[0]

redis_port = 6379

REDIS_CLI = redis.StrictRedis(
    host=redis_host, port=redis_port, decode_responses=True)

def add_process_heart_beat(porcess_id, spider_url):
    REDIS_CLI.lpush('heart_beats', f'heart_beat_of_{porcess_id}_{spider_url}')

def llen_spider():
    return REDIS_CLI.llen('spiders')

def get_spider():
    return REDIS_CLI.rpop('spiders')

def get_active_process(listener_name):
    return REDIS_CLI.get(f'active_process_of_{listener_name}')

def set_active_process(listener_name):
    return REDIS_CLI.set(f'active_process_of_{listener_name}', 0)

def inc_active_process(listener_name):
    REDIS_CLI.incr(f'active_process_of_{listener_name}')

def decr_active_process(listener_name):
    REDIS_CLI.decr(f'active_process_of_{listener_name}')

def start_executor(redis_host, spider_url, porcess_id):
    subprocess.call(["time", "python3", "crawler/main.py", redis_host, spider_url, porcess_id], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
#     subprocess.call(["time", "python3", "crawler/main.py", redis_host, spider_url, porcess_id])
    decr_active_process(listener_name)
    print('Crawling finished ', f'process id : {listener_name}')

subprocess.check_output(["sudo", "rm", "-rf", "shell"])
subprocess.call(["git", "clone", "https://github.com/firedrak/shell.git"])
subprocess.call(["bash", "shell/shell.sh"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

processes = []
listener_name = socket.gethostname()
set_active_process(listener_name)
max_processes = int(os.cpu_count())

print('waiting for spider')

while True:
    if int(get_active_process(listener_name)) < max_processes: 
        if llen_spider():
            spider_url, porcess_id = get_spider().split('_sp_')
            inc_active_process(listener_name)
            print('Crawling started ', f'Process id : {porcess_id}')
            processe = Process(target = start_executor, args = (redis_host, spider_url, porcess_id))
            processe.start()
            add_process_heart_beat(porcess_id, spider_url)
            processes.append(processe)
                                                                
    time.sleep(1)
