#!/usr/bin/python3

from tasks import send_message
from tasks import send_premium
from tasks import send_on_demand
import pymysql
import requests
import json
import time
import sys
import datetime
import logging
import redis

from time import sleep
from requests.packages.urllib3.exceptions import InsecureRequestWarning
# create logger
log_file = datetime.datetime.now().strftime('%d_%m_%Y')
logging.basicConfig(format='%(asctime)s :: %(levelname)s :: %(message)s', filename='/opt/add_to_rabbit_'+str(log_file)+'.log', filemode='w', level=logging.DEBUG)


connection = pymysql.connect(
            host="",
            user="",
            password="",
            db="",
            charset='',
            port=,
            cursorclass=pymysql.cursors.DictCursor
        )
cursor = connection.cursor()

def get_message_content():
    ts = time.time()
    """
        should pick messages that are less than a day old
        otherwise it's just looping through all the unsent messages (with errors and etc) that have recipients
    """
    sql = "SELECT rp.recepient_id as recepient_id , sv.service_type_id as service_type_id ,rp.phone_number as phone_number,sv.OfferCode as OfferCode,mg.message as message, sv.PackageId as PackageId,sc.shortcode as shortcode,mg.message_id as message_id,mg.link_id as link_id,rp.message_params as message_params,sc.network_id as network_id " \
          "FROM recepients rp " \
          "INNER JOIN messages mg ON rp.message_id = mg.message_id " \
          "INNER JOIN services sv ON mg.service_id = sv.service_id " \
          "INNER JOIN shortcodes sc ON sv.shortcode_id = sc.shortcode_id " \
          "WHERE rp.is_picked = 0 AND sv.service_type_id != 7 and rp.status_id != 7 and mg.send_time <= '{}' AND rp.message_params is not null AND (rp.use_server = 2 OR rp.use_server is null) LIMIT 1000;".format(datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S'))

    
    # print(sql)
    cursor.execute(sql)
    list_messages = cursor.fetchall()
    # print(list_messages)
    connection.commit()

    return list_messages

def update_picked_for_processing(recepient_id):
    sql = "UPDATE recepients SET is_picked = 1 WHERE recepient_id = %s;"
    cursor.execute(sql, (recepient_id))
    if cursor.rowcount == 0:
        updated = True
    else:
        updated = True
    connection.commit()
    return updated


if __name__ == '__main__':
    requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
    accessTokenUrl = 'https://dsvc.safaricom.com:9480/api/auth/login';
    data = {
        'username': '',
        'password': ''
    }

    data_json = json.dumps(data)
    payload = {'json_payload': data_json}
    json_data = json.dumps(data)

    headers = {
        'Content-Type': 'application/json',
        'X-Requested-With': 'XMLHttpRequest'
    }
    try:
        r = requests.post(accessTokenUrl, headers=headers, json=data)
        response = json.loads(r.text)
        token = response['token']
        refresh_token = response['refreshToken']
        refresh_time = datetime.datetime.now()
        bearer_text = "Bearer {}".format(token)
        r = redis.Redis()
        r.mset({"bearer_text": bearer_text})
        logging.info("Initial bearer text generated")
    except Exception as e:
        logging.error(e)

    while True:
        # logging.info(str(datetime.datetime.now()) + ":" + str(refresh_time))
        if datetime.datetime.now() >= refresh_time + datetime.timedelta(minutes=30):
            accessTokenUrl = 'https://dsvc.safaricom.com:9480/api/auth/RefreshToken';
            final_bearer_text = ''
            bearer_text = "Bearer+{}".format(refresh_token)
            headers = {
                'Content-Type': 'application/json',
                'X-Requested-With': 'XMLHttpRequest',
                'X-Authorization': bearer_text
            }
            try:
                r = requests.get(accessTokenUrl, headers=headers, verify=False)
                logging.info(r)
                logging.info(r.text)
                # print(r.text)
                if r.text:
                    response = json.loads(r.text)
                    token = response['token']
                    final_bearer_text = "Bearer {}".format(token)
                    r = redis.Redis()
                    r.mset({"bearer_text": final_bearer_text})
                    logging.info("New bearer text generated")
                else:
                    requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
                    accessTokenUrl = 'https://dsvc.safaricom.com:9480/api/auth/login';
                    data = {
                        'username': '',
                        'password': ''
                    }

                    data_json = json.dumps(data)
                    payload = {'json_payload': data_json}
                    json_data = json.dumps(data)

                    headers = {
                        'Content-Type': 'application/json',
                        'X-Requested-With': 'XMLHttpRequest'
                    }
                    try:
                        r = requests.post(accessTokenUrl, headers=headers, json=data)
                        response = json.loads(r.text)
                        token = response['token']
                        refresh_token = response['refreshToken']
                        refresh_time = datetime.datetime.now()
                        bearer_text = "Bearer {}".format(token)
                        r = redis.Redis()
                        r.mset({"bearer_text": bearer_text})
                        logging.info("Initial bearer text generated")
                    except Exception as e:
                        logging.error(e)
            except Exception as e:
                logging.error(e)
            # bearer_text = final_bearer_text
            refresh_time = datetime.datetime.now()

        sms = get_message_content()
        # print(sms)
        if len(sms) > 0:
            for message_content in sms:

                message_json = []
                if message_content["message_params"]:
                    # {"phone":"734234346","name":"Lisa","amount":"2300","date":"20/7/2022","country":"Tanzania"}
                    message_params = json.loads(message_content["message_params"])
                    try:
                        for keys in message_params.keys():
                            message_content["message"] = message_content["message"].replace("{"+ str(keys) +"}", message_params[keys])
                    except Exception as e:
                        r = redis.Redis()
                        r.mset({"recepient_" + str(message_content["recepient_id"]): "1"})
                        logging.error(e)

                if message_content["service_type_id"] == 6:
                    #sending bulk messages
                    if update_picked_for_processing(message_content["recepient_id"]):
                        send_message.apply_async(args=[json.dumps(message_content)],priority=0)
                        # if message_content["message_id"] == 13:
                        #     sql = "SELECT short_message FROM smpp_log WHERE message_id = '{}' ;".format("smpp-{}".format(message_content["recepient_id"]))
                        #     print(sql)
                        #     cursor.execute(sql)
                        #     message_content_message = cursor.fetchone()
                        #     connection.commit()
                        #     message_content['message'] = message_content_message['short_message']

                elif message_content["service_type_id"] == 5:
                    #sending premium/subscription messages
                    if update_picked_for_processing(message_content["recepient_id"]):
                        send_premium.apply_async(args=[json.dumps(message_content)])
                elif message_content["service_type_id"] == 1:
                    #sending on-demand messages
                    # update_picked_for_processing(message_content["recepient_id"])
                    if update_picked_for_processing(message_content["recepient_id"]):
                        send_on_demand.apply_async(args=[json.dumps(message_content)])
                elif message_content["service_type_id"] == 7:
                    #sending on-demand messages
                    # update_picked_for_processing(message_content["recepient_id"])
                    if update_picked_for_processing(message_content["recepient_id"]):
                        send_on_demand.apply_async(args=[json.dumps(message_content)])
