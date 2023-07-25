#!/usr/bin/python3
"""
Only picks saf contacts in batches of 20

"""
# (1)
# https://www.ibm.com/docs/en/watson-studio-local/1.2.3?topic=practices-access-large-data
from __future__ import generators

from tasks import send_message_batch
# from tasks import send_premium
# from tasks import send_on_demand
import pymysql
import requests
import json
import time
import sys
import datetime
import logging
import redis
import os

# (2)
# https://www.velotio.com/engineering-blog/async-features-in-python
import asyncio

from time import sleep
from datetime import timedelta
from requests.packages.urllib3.exceptions import InsecureRequestWarning

# create logger
log_file = datetime.datetime.now().strftime('%d_%m_%Y')
logging.basicConfig(format='%(asctime)s :: %(levelname)s :: %(message)s', filename='/opt/add_to_rabbit_'+str(log_file)+'.log', filemode='w', level=logging.DEBUG)


connection = pymysql.connect(
            host="db-mysql-nyc3-30176-do-user-7525826-0.b.db.ondigitalocean.com",
            user="tfm_sms_user",
            password="AVNS_ydDAvBjHy1Hn3qNqTOx",
            db="tm_prsp_db",
            charset='utf8mb4',
            port=25060,
            cursorclass=pymysql.cursors.DictCursor
        )
cursor = connection.cursor()


# (1) - a
def ResultIterator(cursor, arraysize=1000):
    'iterator using fetchmany and consumes less memory'
    while True:
        results = cursor.fetchmany(arraysize)
        if not results:
            break
        for result in results:
            yield result

# (1) - b
def get_message_content():
    ts = time.time()

    #last 8 hours
    time_limit = datetime.datetime.fromtimestamp(ts) - timedelta(hours = 8)


    # sql = " SELECT mg.message as message, mg.message_id as message_id,mg.link_id as link_id, sv.OfferCode as OfferCode,sv.PackageId as PackageId,sc.shortcode as shortcode,sc.network_id as network_id FROM messages_test mg "\
    #         "INNER JOIN recepients_test rp ON mg.message_id = rp.message_id " \
    #         "INNER JOIN services sv ON mg.service_id = sv.service_id "\
    #         "INNER JOIN shortcodes sc ON sv.shortcode_id = sc.shortcode_id "\
    #         "WHERE LENGTH(mg.message) > 0 AND rp.is_picked='0' AND rp.message_params is null AND sv.service_type_id = 6 and (send_time >'{}'  and send_time <= '{}')".format(time_limit.strftime('%Y-%m-%d %H:%M:%S'),datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S'))

    # include fremium services
    # sql = "SELECT sv.service_type_id, rp.recepient_id as recepient_id , sv.service_type_id as service_type_id ,rp.phone_number as phone_number,sv.OfferCode as OfferCode,mg.message as message, sv.PackageId as PackageId,sc.shortcode as shortcode,mg.message_id as message_id,mg.link_id as link_id,rp.message_params as message_params,sc.network_id as network_id " \
    #       "FROM recepients rp " \
    #       "INNER JOIN messages mg ON rp.message_id = mg.message_id " \
    #       "INNER JOIN services sv ON mg.service_id = sv.service_id " \
    #       "INNER JOIN shortcodes sc ON sv.shortcode_id = sc.shortcode_id " \
    #       "WHERE LENGTH(mg.message) > 0 AND rp.is_picked = 0 AND sv.service_type_id = 6 and rp.message_params is null and" \
    #       "(SUBSTRING(rp.phone_number,4,3) != '762' OR SUBSTRING(rp.phone_number,4,3) != '792' OR SUBSTRING(rp.phone_number,4,3) != '730' OR SUBSTRING(rp.phone_number,4,3) != '731' OR SUBSTRING(rp.phone_number,4,3) != '732' OR SUBSTRING(rp.phone_number,4,3) != '733' OR SUBSTRING(rp.phone_number,4,3) != '734' OR SUBSTRING(rp.phone_number,4,3) != '735' OR SUBSTRING(rp.phone_number,4,3) != '736' OR SUBSTRING(rp.phone_number,4,3) != '737' OR SUBSTRING(rp.phone_number,4,3) != '738' OR SUBSTRING(rp.phone_number,4,3) != '739' OR SUBSTRING(rp.phone_number,4,3) != '750' OR SUBSTRING(rp.phone_number,4,3) != '751' OR SUBSTRING(rp.phone_number,4,3) != '752' OR SUBSTRING(rp.phone_number,4,3) != '753' OR SUBSTRING(rp.phone_number,4,3) != '754' OR SUBSTRING(rp.phone_number,4,3) != '755' OR SUBSTRING(rp.phone_number,4,3) != '756' OR SUBSTRING(rp.phone_number,4,3) != '780' OR SUBSTRING(rp.phone_number,4,3) != '781' OR SUBSTRING(rp.phone_number,4,3) != '782' OR SUBSTRING(rp.phone_number,4,3) != '785' OR SUBSTRING(rp.phone_number,4,3) != '786' OR SUBSTRING(rp.phone_number,4,3) != '787' OR SUBSTRING(rp.phone_number,4,3) != '788' OR SUBSTRING(rp.phone_number,4,3) != '789' OR SUBSTRING(rp.phone_number,4,3) != '100' OR SUBSTRING(rp.phone_number,4,3) != '101' OR SUBSTRING(rp.phone_number,4,3) != '102')" \
    #       "AND rp.status_id != 7 AND (services.service_type_id=5 or services.service_type_id=7) AND (rp.use_server=2 OR rp.use_server is null) AND  send_time <= '{}';".format(datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S'))

    sql = "SELECT rp.recepient_id as recepient_id , sv.service_type_id as service_type_id ,rp.phone_number as phone_number,sv.OfferCode as OfferCode,mg.message as message, sv.PackageId as PackageId,sc.shortcode as shortcode,mg.message_id as message_id,mg.link_id as link_id,rp.message_params as message_params,sc.network_id as network_id " \
          "FROM recepients rp " \
          "INNER JOIN messages mg ON rp.message_id = mg.message_id " \
          "INNER JOIN services sv ON mg.service_id = sv.service_id " \
          "INNER JOIN shortcodes sc ON sv.shortcode_id = sc.shortcode_id " \
          "WHERE LENGTH(mg.message) > 0 AND rp.is_picked = 0 AND sv.service_type_id = 6 and rp.message_params is null and" \
          "(SUBSTRING(rp.phone_number,4,3) != '762' OR SUBSTRING(rp.phone_number,4,3) != '792' OR SUBSTRING(rp.phone_number,4,3) != '730' OR SUBSTRING(rp.phone_number,4,3) != '731' OR SUBSTRING(rp.phone_number,4,3) != '732' OR SUBSTRING(rp.phone_number,4,3) != '733' OR SUBSTRING(rp.phone_number,4,3) != '734' OR SUBSTRING(rp.phone_number,4,3) != '735' OR SUBSTRING(rp.phone_number,4,3) != '736' OR SUBSTRING(rp.phone_number,4,3) != '737' OR SUBSTRING(rp.phone_number,4,3) != '738' OR SUBSTRING(rp.phone_number,4,3) != '739' OR SUBSTRING(rp.phone_number,4,3) != '750' OR SUBSTRING(rp.phone_number,4,3) != '751' OR SUBSTRING(rp.phone_number,4,3) != '752' OR SUBSTRING(rp.phone_number,4,3) != '753' OR SUBSTRING(rp.phone_number,4,3) != '754' OR SUBSTRING(rp.phone_number,4,3) != '755' OR SUBSTRING(rp.phone_number,4,3) != '756' OR SUBSTRING(rp.phone_number,4,3) != '780' OR SUBSTRING(rp.phone_number,4,3) != '781' OR SUBSTRING(rp.phone_number,4,3) != '782' OR SUBSTRING(rp.phone_number,4,3) != '785' OR SUBSTRING(rp.phone_number,4,3) != '786' OR SUBSTRING(rp.phone_number,4,3) != '787' OR SUBSTRING(rp.phone_number,4,3) != '788' OR SUBSTRING(rp.phone_number,4,3) != '789' OR SUBSTRING(rp.phone_number,4,3) != '100' OR SUBSTRING(rp.phone_number,4,3) != '101' OR SUBSTRING(rp.phone_number,4,3) != '102')" \
          "AND rp.status_id != 7 AND (rp.use_server=2 OR rp.use_server is null) AND  send_time <= '{}';".format(datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S'))

    # sql = "SELECT rp.recepient_id as recepient_id , sv.service_type_id as service_type_id ,rp.phone_number as phone_number,sv.OfferCode as OfferCode,mg.message as message, sv.PackageId as PackageId,sc.shortcode as shortcode,mg.message_id as message_id,mg.link_id as link_id,rp.message_params as message_params,sc.network_id as network_id " \
    #       "FROM recepients rp " \
    #       "INNER JOIN messages mg ON rp.message_id = mg.message_id " \
    #       "INNER JOIN services sv ON mg.service_id = sv.service_id " \
    #       "INNER JOIN shortcodes sc ON sv.shortcode_id = sc.shortcode_id " \
    #       "WHERE LENGTH(mg.message) > 0 AND rp.is_picked = 0 AND sv.service_type_id = 6 and rp.message_params is null and" \
    #       "(SUBSTRING(rp.phone_number,4,3) != '762' OR SUBSTRING(rp.phone_number,4,3) != '792' OR SUBSTRING(rp.phone_number,4,3) != '730' OR SUBSTRING(rp.phone_number,4,3) != '731' OR SUBSTRING(rp.phone_number,4,3) != '732' OR SUBSTRING(rp.phone_number,4,3) != '733' OR SUBSTRING(rp.phone_number,4,3) != '734' OR SUBSTRING(rp.phone_number,4,3) != '735' OR SUBSTRING(rp.phone_number,4,3) != '736' OR SUBSTRING(rp.phone_number,4,3) != '737' OR SUBSTRING(rp.phone_number,4,3) != '738' OR SUBSTRING(rp.phone_number,4,3) != '739' OR SUBSTRING(rp.phone_number,4,3) != '750' OR SUBSTRING(rp.phone_number,4,3) != '751' OR SUBSTRING(rp.phone_number,4,3) != '752' OR SUBSTRING(rp.phone_number,4,3) != '753' OR SUBSTRING(rp.phone_number,4,3) != '754' OR SUBSTRING(rp.phone_number,4,3) != '755' OR SUBSTRING(rp.phone_number,4,3) != '756' OR SUBSTRING(rp.phone_number,4,3) != '780' OR SUBSTRING(rp.phone_number,4,3) != '781' OR SUBSTRING(rp.phone_number,4,3) != '782' OR SUBSTRING(rp.phone_number,4,3) != '785' OR SUBSTRING(rp.phone_number,4,3) != '786' OR SUBSTRING(rp.phone_number,4,3) != '787' OR SUBSTRING(rp.phone_number,4,3) != '788' OR SUBSTRING(rp.phone_number,4,3) != '789' OR SUBSTRING(rp.phone_number,4,3) != '100' OR SUBSTRING(rp.phone_number,4,3) != '101' OR SUBSTRING(rp.phone_number,4,3) != '102')" \
    #       " AND rp.status_id != 7  AND  send_time <= '{}';".format(datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S'))

    # print(sql)
    cursor.execute(sql)
    
    return cursor
    

async def async_update_picked_for_processing(recepient_id):
    sql = "UPDATE recepients SET is_picked = 1 WHERE recepient_id = %s;"
    cursor.execute(sql, (recepient_id))
    if cursor.rowcount == 0:
        updated = True
    else:
        updated = True
    connection.commit()

    await asyncio.sleep(2/1000)

async def update_main_picked(recepient_id):
    batch = asyncio.create_task(async_update_picked_for_processing(recepient_id))
    await batch

async def async_batch_update_picked_for_processing(recepient_ids):
    sql = "UPDATE recepients SET is_picked = 1 WHERE recepient_id IN {};".format(recepient_ids)
    
    # print(sql)

    cursor.execute(sql)
    if cursor.rowcount == 0:
        updated = True
    else:
        updated = True
    connection.commit()
    
    # milliseconds
    await asyncio.sleep(2/1000)

    
async def update_main_batch(recepient_ids):
    batch = asyncio.create_task(async_batch_update_picked_for_processing(recepient_ids))
    await batch


if __name__ == '__main__':
    while True:
        sms = get_message_content() # SAFARICOM ONLY

        list_of_msisdn       = []
        list_of_ids          = []
        list_of_PackageId    = []
        list_of_shortcode    = []
        list_of_messages     = []

        message = {}
        message['phone_number']     = []
        message['list_of_ids']      = []
        message['list_of_PackageId'] = []
        message['list_of_shortcode'] = []
        message['list_of_messages'] = []
        
        x =0

        if cursor.rowcount:
            print("== DATA FOUND IN DB ========================")
            print('== Data Count: ==',cursor.rowcount,' =======================')
            
            for message_content in ResultIterator(sms):
                

                list_of_msisdn.append(str(message_content['phone_number']))
                list_of_ids.append(str(message_content['recepient_id']))
                list_of_messages.append(str(message_content['message']))
                
                if x == 20:
                
                    message["phone_number"] = ','.join(list_of_msisdn)
                    message["list_of_ids"] = list_of_ids
                    message['list_of_messages']    = list_of_messages

                    message['message_id']        = message_content['message_id']
                    message['service_type_id']   = message_content['service_type_id']
                    message['OfferCode']         = message_content['OfferCode']
                    message['PackageId']         = message_content['PackageId']
                    message['shortcode']         = message_content['shortcode']
                    message['link_id']           = message_content['link_id']
                    message['network_id']        = message_content['network_id']
                    message['message'] = message_content['message']
                    

                    print('phone_number list=====> ',message['phone_number'])
                    print('recipient_id list=====> ',message['list_of_ids'])
                   
                    send_message_batch.apply_async(args=[json.dumps(message)], priority=0)

                    asyncio.run(update_main_batch(tuple(list_of_ids)))

                    list_of_msisdn       = []
                    list_of_ids          = []
                    list_of_messages     = []

                    message['phone_number']     = []
                    message['list_of_ids']      = []
                    message['list_of_messages'] = []

                    x=0
                elif cursor.rowcount >= 1 and cursor.rowcount < 20:
                
                    print('== Found less than 20 ======================')
                    
                    if cursor.rowcount == 1:
                        print('== Found one ===============================')
                        
                        message["phone_number"] = message_content['phone_number']
                        message["list_of_ids"] = list_of_ids
                        message['list_of_messages']    = list_of_messages

                        message['message_id']        = message_content['message_id']
                        message['service_type_id']   = message_content['service_type_id']
                        message['OfferCode']         = message_content['OfferCode']
                        message['PackageId']         = message_content['PackageId']
                        message['shortcode']         = message_content['shortcode']
                        message['link_id']           = message_content['link_id']
                        message['network_id']        = message_content['network_id']

                        message['message'] = message_content['message']
                        

                        print('phone_number list=====> ',message['phone_number'])
                        print('recipient_id list=====> ',message['list_of_ids'])
                        # print('message =====> ',message['message'])
                        
                        send_message_batch.apply_async(args=[json.dumps(message)], priority=0)
                        asyncio.run(update_main_picked(message_content['recepient_id']))

                        list_of_msisdn       = []
                        list_of_ids          = []
                        list_of_messages     = []

                        message = {}
                        message['phone_number']     = []
                        message['list_of_ids']      = []
                        message['list_of_messages'] = []
                        
                    else:
                        print('== ',cursor.rowcount,' rows remaining =====================')
                        message["phone_number"] = ','.join(list_of_msisdn)
                        message["list_of_ids"] = list_of_ids
                        message['list_of_messages']    = list_of_messages

                        if len(list_of_ids) == cursor.rowcount:

                            message['message_id']        = message_content['message_id']
                            message['service_type_id']   = message_content['service_type_id']
                            message['OfferCode']         = message_content['OfferCode']
                            message['PackageId']         = message_content['PackageId']
                            message['shortcode']         = message_content['shortcode']
                            message['link_id']           = message_content['link_id']
                            message['network_id']        = message_content['network_id']

                            message['message'] = message_content['message']
                            

                            print('phone_number list=====> ',message['phone_number'])
                            print('recipient_id list=====> ',message['list_of_ids'])
                            # print('message =====> ',message['message'])
                            
                            send_message_batch.apply_async(args=[json.dumps(message)], priority=0)

                            asyncio.run(update_main_batch(tuple(list_of_ids)))

                            list_of_msisdn       = []
                            list_of_ids          = []
                            list_of_messages     = []

                            message = {}
                            message['phone_number']     = []
                            message['list_of_ids']      = []
                            message['list_of_messages'] = []


                # print(x,'==============================')
                x+=1
        else:
            # print("nothing found")
            os.execv(sys.argv[0], sys.argv)
            # continue

