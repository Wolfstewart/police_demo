from pykafka import KafkaClient
import json
from datetime import datetime
import uuid
import time
import os
from multiprocessing import Process


# READ COORDINATES FROM GEOJSON
def get_coordinates(input_file):
    input_file = open(input_file)
    json_array = json.load(input_file)
    coordinates = json_array['features'][0]['geometry']['coordinates']
    return coordinates


# GENERATE UUID
def generate_uuid():
    return uuid.uuid4()


def generate_checkpoint(coordinates, hosts, topic_str):
    # client = KafkaClient(hosts=hosts)
    print(f'{topic_str} 开始启动！！')
    # topic = client.topics[topic_str]
    # producer = topic.get_sync_producer()
    # CONSTRUCT MESSAGE AND SEND IT TO KAFKA
    data = {}
    data['police_line'] = topic_str
    i = 0
    while i < len(coordinates):
        data['key'] = data['police_line'] + '_' + str(generate_uuid())
        data['timestamp'] = str(datetime.utcnow())
        data['latitude'] = coordinates[i][1]
        data['longitude'] = coordinates[i][0]
        message = json.dumps(data)
        print(message)
        # producer.produce(message.encode('ascii'))
        time.sleep(1)

        # if bus reaches last coordinate, start from beginning
        if i == len(coordinates) - 1:
            i = 0
        else:
            i += 1


# generate_checkpoint(coordinates)


if __name__ == '__main__':

    data_path = './data'
    data_files = [os.path.join(data_path,file) for file in os.listdir(data_path)]

    # 连接地址和topics
    topics = ['person_0', 'person_1', 'person_2']
    hosts = ''

    num = 3
    process_list = []
    for i in range(3):
        # data_file = os.path.join(data_path, data_files[i])
        coordinates = get_coordinates(data_files[i])
        # print(data_files[i])
        # print(coordinates)
        # print(topics[i])

        p = Process(target=generate_checkpoint, args=(coordinates, hosts, topics[i]))
        p.start()
        process_list.append(p)

    for p in process_list:
        p.join()
