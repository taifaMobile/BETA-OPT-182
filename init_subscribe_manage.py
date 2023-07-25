#!/usr/bin/python3

"""
Initiate subscribe manage service activation
For safaricom only

"""

from __future__ import generators

from tasks import init_subscribe_manage
from tasks import bill_subscribe_manage

import pymysql
import requests
import json
import time
import sys
import datetime
import logging
import redis
import os

# import asyncio

from time import sleep
from datetime import timedelta
from requests.packages.urllib3.exceptions import InsecureRequestWarning

# create logger
log_file = datetime.datetime.now().strftime('%d_%m_%Y')
logging.basicConfig(format='%(asctime)s :: %(levelname)s :: %(message)s', filename='/opt/init_subscribe_manage_'+str(log_file)+'.log', filemode='w', level=logging.DEBUG)

connection = pymysql.connect(
            host="",
            user="",
            password="",
            db="",
            charset='utf8mb4',
            port=,
            cursorclass=pymysql.cursors.DictCursor
        )
cursor = connection.cursor()

def get_phone_numbers():
    sql = "SELECT * FROM sub_requests"

    cursor.execute(sql)
    list_phone_numbers = cursor.fetchall()
    connection.commit()

    return list_phone_numbers

def clean_req_subs_table(id):
    sql = "DELETE FROM sub_requests WHERE id = %s;"
    cursor.execute(sql, (id))

    connection.commit()


if __name__ == '__main__':
    requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

    while True:

        sub_requests = get_phone_numbers()

        if sub_requests:
            for phone in sub_requests:
                # 1 => subscription
                # 2 => billing
                if phone['sub_type'] == '1':
                    init_subscribe_manage.apply_async(args=[json.dumps(phone)])
                    clean_req_subs_table(phone["id"])
                else:
                    bill_subscribe_manage.apply_async(args=[json.dumps(phone)])
                    clean_req_subs_table(phone["id"])
                
                


                