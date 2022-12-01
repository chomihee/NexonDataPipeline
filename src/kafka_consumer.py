from kafka import KafkaConsumer
from json import loads
import os

from kafka_config import CONFIG


def diff(list1, list2):
    list3 = [value for value in list1 if value not in list2]
    return list3


# topic, broker list
consumer = KafkaConsumer(
    'test',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='dev-group',
    value_deserializer=lambda x: loads(x.decode('utf-8')),
    consumer_timeout_ms=6000
)

print('[begin] get consumer list')


# value: 파일명 , 해당 ID가 각 한줄씩 저장됨, 받은 데이터 파일의 넣을 때 중복이면 추가 안함
# 파일 내용 한번에 다 읽고 존재안하면 input 이후 파일 close
# 1. 파일명이 존재 하지 않으면 > 파일 생성 및 write
# 2. 존재하는 경우 > 기존 ID 있나 확인하고 없는것만 write
# !!! 중복처리는 elasticsearch 활용 : 추후 예정
def file_write(id_list):
    match_type = list(id_list.keys())[0]
    append_list = id_list.get(match_type)
    # 파일 존재 여부 확인 (있다고 전제)
    file_name = CONFIG["file_dir"] + match_type + '.txt'
    file_exist = True
    if not os.path.isfile(file_name):
        file_exist = False
    print("file_exist : " + str(file_exist))

    if file_exist:
        # 파일을 읽어서 존재하지 않는 것만 추가하고 다시 담기
        with open(file_name) as f:
            asis_id_list = f.read().splitlines()

        # 기존 list에 신규 list가 완전 포함이지 않으면 logic 돌아감
        diff_list = diff(append_list, asis_id_list)
        if diff_list:
            print('Append Data : ' + str(len(diff_list)))
            # 다시 피클로 저장
            tobe_id_list = asis_id_list + diff_list
            with open(file_name, 'a') as file:
                file.write("\n")
                file.write("\n".join(tobe_id_list))
        else:
            print('Insert Pass')

    else:  # 파일이 존재하지 않으면 단순 추가
        print('Add Data')
        with open(file_name, 'w') as file:
            file.write("\n".join(append_list))


for message in consumer:
    print("Topic %s, Partition: %d, Offset: %d, Key, %s, value: %s" %
          (message.topic, message.partition, message.offset, message.key, message.value))

    # 받은 데이터 파일로 적재
    file_write(message.value)

print('[end] get consumer list')
