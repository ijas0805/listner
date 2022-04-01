import csv
import json
from settings import redisCli, OUT_PUT_FILE_NAME

data_len = redisCli.REDIS_CLI.llen('data')

with open(OUT_PUT_FILE_NAME, 'w') as csvfile:
    csvwriter = csv.writer(csvfile)

    def redis_pop(key):
        return json.loads(redisCli.REDIS_CLI.lpop(key))

    while redisCli.REDIS_CLI.llen('data'):
        item = redis_pop('data')[0].items()
        csvwriter.writerow(item)