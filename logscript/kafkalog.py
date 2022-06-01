from kafka import KafkaConsumer
from json import loads
from datetime import datetime
import time
import csv

def recordLog(messsage):
    log = [message.value['querier'], message.value['operation'], message.value['result']]
    with open(fname, 'a', newline='')as write_file:
        writer = csv.writer(write_file)
        writer.writerow(log)

def deleteLogs(id):
    print('id: ', id)
    lines = []
    with open(fname, 'r') as read_file:
        reader = csv.reader(read_file)
        for row in reader:
            if row[0] != id:
                lines.append(row)
    with open(fname, 'w') as write_file:
        writer = csv.writer(write_file)
        writer.writerows(lines)

ts = time.time()
fname = 'log' + str(datetime.fromtimestamp(ts)) + '.csv'
with open(fname, 'x') as f:
    print('new log file opened')

consumer = KafkaConsumer(
    'logResults',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')))

for message in consumer:
    msg = message.value
    if msg['delete'] == 'false':
        recordLog(msg)
    else:
        deleteLogs(msg['querier'])