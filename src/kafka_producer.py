from kafka import KafkaProducer
from json import dumps
import requests
import datetime
import time

from kafka_config import CONFIG

producer = KafkaProducer(
    acks=0,
    compression_type='gzip',
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: dumps(x).encode('utf-8')
)

start_date = ""
end_date = ""
offset = ""
limit = ""
match_types = {
    "item_sole": CONFIG.get('gameType')[0].get('id'),  # 아이템 개인전
    "item_defence": CONFIG.get('gameType')[1].get('id'),  # 아이템 막자
    "item_team": CONFIG.get('gameType')[2].get('id'),  # 아이템 팀 배틀모드
    "item_mini_solo": CONFIG.get('gameType')[3].get('id'),  # 아이템 미니 개인전
    "speed_defence": CONFIG.get('gameType')[4].get('id'),  # 스피드 막자
    "deathmatch_solo": CONFIG.get('gameType')[5].get('id'),  # 데스매치 개인전

}
match_type_list = list(match_types.keys())
header = {'Authorization': CONFIG["api_key"]}
# 1초에 5건씩 끊어서 요청
# start time-end time 5개 전송해서 보내고 1초가 초과되지않는다면 1초될때까지 기다린 후 재실행
index = 0
count = 0
start_time = datetime.datetime.now()
while True:
    # count 가 0인 경우 시작시간 계산
    if count == 0:
        start_time = datetime.datetime.now()

    match_type = match_type_list[index]
    print(match_type)
    url = 'https://api.nexon.co.kr/kart/v1.0/matches/all?start_date=&end_date=&offset=&limit=&match_types=' + match_types.get(
        match_type)
    response = requests.get(url, headers=header)
    # return이 null인 경우 처리
    try:
        match_IDs = response.json().get('matches')[0].get('matches')
        data = {match_type: match_IDs}
        kaf_reponse = producer.send('test', value=data).get()
        producer.flush()
    except TypeError:
        print(match_type + " : return 0, Need remove match type")
        pass

    index = index + 1
    count = count + 1

    # count 4면 api 0~4번까지 보낼까지 시간을 확인
    if count == 4:
        check_time = datetime.datetime.now()
        diff_time = check_time - start_time
        # 1초 경과시 그냥 초기화
        if diff_time.seconds >= 1:
            count = 0
        # 미경과시 1초기다렸다 초기화
        else:
            wait = datetime.timedelta(seconds=1) - diff_time
            time.sleep(wait.total_seconds())
            print("wait " + str(wait.total_seconds()) + "ms")
            count = 0

    # match_type 이 끝까지 돌았다면 초기화
    if index == len(match_type_list):
        index = 0
