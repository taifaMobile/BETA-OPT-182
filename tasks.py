from celery import Celery
import requests
import json
import pymysql.cursors
import datetime
import time
import sys
import africastalking
import redis
import urllib


from requests.packages.urllib3.exceptions import InsecureRequestWarning
import logging

app = Celery('tasks',broker='pyamqp://guest@localhost//')

@app.task
def send_message(body):
    log_file = datetime.datetime.now().strftime('%d_%m_%Y')
    logging.basicConfig(format='%(asctime)s :: %(levelname)s :: %(message)s', filename='/opt/tasks_log_' + str(log_file) + '.log', filemode='a', level=logging.DEBUG)
    requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
    try:
        #body = json.loads(body)
        send_response = ""
        message_params = json.loads(body)
        # print(message_params)
        #print(message_params["isp"])
        prefix = message_params['phone_number'][3:6]
        #print(prefix)
        if prefix in [ "762","792"]:
            isp = 2
        elif prefix in ["730","731","732","733","734","735","736","737","738","739","750","751","752","753","754","755","756","780","781","782","785","786","787","788","789","100","101","102"]:
            isp = 3
            #isp = 2
        else:
            isp = 1

        # print(isp)
        if isp == 1:

            BulkSendUrl = 'https://dsvc.safaricom.com:9480/api/public/CMS/bulksms';
            r = redis.Redis()

            # print(str(r.get('bearer_text').decode()))
            # headers_bulk = {
            #     'X-Authorization': message_params['bearer_text']
            # }
            headers_bulk = {
                'X-Authorization':  str(r.get('bearer_text').decode())
            }

            data = {
                "timeStamp" : datetime.datetime.now().strftime("%s"),
                "dataSet" : [{
                    "userName":  "taifamobile",
                    "channel" :  "sms",
                    "packageId" :  message_params['PackageId'],
                    "oa" :  message_params['shortcode'],
                    "msisdn" :  message_params['phone_number'],
                    "message" :  "{}".format(message_params['message']),
                    "uniqueId" :  message_params['recepient_id'],
                    "actionResponseURL" : ""

                }]
            }
            data = json.dumps(data)
            # print(data)
            try:
                sendBulk = requests.post(BulkSendUrl, headers=headers_bulk,data=data, timeout=1)
                if not sendBulk.text:
                    print("Empty response.")
                    r.mset({"recepient_" + str(message_params["recepient_id"]): "1"})
                    # sendBulk.close()
                else:
                    response_bulk = json.loads(sendBulk.text)
                    logging.error('============ Phone: '+str(message_params['phone_number']))
                    logging.error(response_bulk)
                    # print(headers_bulk)
                    # print(response_bulk)
                    # sendBulk.close()
                    # if response_bulk["statusCode"] == "SC0000":
                    r.mset({"recepient_" + str(message_params["recepient_id"]): "1"})
            except Exception as e:
                r.mset({"recepient_" + str(message_params["recepient_id"]): "1"})
                logging.error(e)
                # raise SystemExit(e)
            # print(sendBulk.text)
        elif isp == 2:
            r = redis.Redis()
            if message_params["network_id"] == 3:
                try:
                    username = ""
                    api_key = ""

                    africastalking.initialize(username, api_key)

                    sms = africastalking.SMS

                    Url = "https://api.africastalking.com/version1/messaging"
                    headers = {
                        'Content-Type': 'application/x-www-form-urlencoded',
                        'Accept': 'application/json',
                        'apiKey': ''
                    }
                    recipients = ["+{}".format(message_params['phone_number'])]
                    # Set your message
                    message = "{}".format(message_params['message']);
                    # Set your shortCode or se
                    sender = ""

                    response = sms.send(message, recipients, sender)
                    # print(response)

                    if not response:
                        print("Empty response.")
                    else:
                        # response_bulk = json.loads(response)
                        if response['SMSMessageData']['Recipients'][0]['status'] == "Success":
                            r = redis.Redis()
                            r.mset({message_params["recepient_id"]: "1"})

                except Exception as e:
                    logging.error(e)
            r.mset({"recepient_" + str(message_params["recepient_id"]): "1"})
        elif isp == 3:
            r = redis.Redis()
            if message_params["network_id"] == 2:
                Url = 
                print(Url)
                request = requests.get(Url)
                # print(r.text)
                response = request.text.split('|')
                # print(response[0])
                if not response:
                    print("Empty response.")
                else:
                    # response_bulk = json.loads(response)
                    if response[0] == "+OK":
                        r.mset({"recepient_" + str(message_params["recepient_id"]): "1"})
            r.mset({"recepient_" + str(message_params["recepient_id"]): "1"})

    except Exception as e:
        logging.error(e)
        # update_db_send(0,message_params["message_id"],message_params["recepient_id"])
    #return send_response

@app.task
def send_premium(body):
    #body = json.loads(body)
    try:

        message_params = json.loads(body)
        #print(message_params)
        r = redis.Redis()
        SendUrl = 'https://dsvc2.safaricom.com:8480/api/public/SDP/sendSMSRequest';
        #print(message_params["refresh_token"])


        headers_bulk = {
            'Content-Type': 'application/json',
            'X-Requested-With': 'XMLHttpRequest',
            'X-Authorization': str(r.get('bearer_text').decode())
        }

        data = {
            "requestId" : message_params['recepient_id'],
            # "requestTimeStamp": datetime.datetime.now().strftime("%s"),
            "channel"   : "APIGW",
            "requestParam" : {
                "data" : [{"name":"LinkId", "value" : ''},
                          {"name": "Msisdn", "value" : message_params['phone_number']},
                          {"name": "OfferCode", "value" : message_params['OfferCode']},
                          {"name": "Content" , "value" : message_params['message']},
                          {"name": "CpId", "value" : "50"}]},
            "operation" : "SendSMS",
            "SourceAddress": ""
        }

        #print(data)
        data = json.dumps(data)

        #print(data)
        sendBulk = requests.post(SendUrl, headers=headers_bulk,data=data)
        #print(sendBulk.text)
        if not sendBulk.text :
            print("Empty response.")
        else:
            response_bulk = json.loads(sendBulk.text)
            if response_bulk["responseParam"]["statusCode"] == "849":
                r = redis.Redis()
                r.mset({message_params["recepient_id"]: "1"})

        #for message_params in body :
    except:
        logging.error(e)

@app.task
def send_on_demand(body):
    try:
        r = redis.Redis()

        message_params = json.loads(body)

        SendUrl = 'https://dsvc.safaricom.com:9480/api/public/SDP/sendSMSRequest';
        bearer_text = str(r.get('bearer_text').decode())

        headers_bulk = {
            'Content-Type': 'application/json',
            'X-Requested-With': 'XMLHttpRequest',
            'X-Authorization': bearer_text
        }

        data = {
            "requestId": message_params['recepient_id'],
            "channel": "APIGW",
            "requestTimeStamp": datetime.datetime.now().strftime("%Y%M%d%H%M%S"),
            "requestParam": {
                "data": [{"name": "LinkId", "value": message_params['link_id']},
                         {"name": "Msisdn", "value": message_params['phone_number']},
                         {"name": "OfferCode", "value": message_params['OfferCode']},
                         {"name": "Content", "value": message_params['message']},
                         {"name": "CpId", "value": "50"}]},
            "operation": "SendSMS"
        }

        data = json.dumps(data)

        # sendBulk = requests.post(SendUrl, headers=headers_bulk, data=data)
        # print(sendBulk.text)
        # if not sendBulk.text:
        #     print("Empty response.")
        # else:
        #     response_bulk = json.loads(sendBulk.text)
            # if response_bulk["responseParam"]["statusCode"] == "849":
        r.mset({message_params["recepient_id"]: "1"})

    except Exception as e:
        logging.error(e)


@app.task
def send_message_batch(body):
    log_file = datetime.datetime.now().strftime('%d_%m_%Y')
    logging.basicConfig(format='%(asctime)s :: %(levelname)s :: %(message)s', filename='/opt/tasks_log_' + str(log_file) + '.log', filemode='a', level=logging.DEBUG)
    requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
    try:
        #body = json.loads(body)
        send_response = ""
        message_params = json.loads(body)
        BulkSendUrl = 'https://dsvc.safaricom.com:9480/api/public/CMS/bulksms';
        r = redis.Redis()

        # print(str(r.get('bearer_text').decode()))
        # headers_bulk = {
        #     'X-Authorization': message_params['bearer_text']
        # }
        headers_bulk = {
            'X-Authorization':  str(r.get('bearer_text').decode())
        }

        data = {
            "timeStamp" : datetime.datetime.now().strftime("%s"),
            "dataSet" : [{
                "userName":  "taifamobile",
                "channel" :  "sms",
                "packageId" :  message_params['PackageId'],
                "oa" :  message_params['shortcode'],
                "msisdn" :  message_params['phone_number'],
                "message" :  "{}".format(message_params['message']),
                "uniqueId" :  message_params['message_id'],
                "actionResponseURL" :  ""

            }]
        }
        data = json.dumps(data)
        print(data)
        try:
            sendBulk = requests.post(BulkSendUrl, headers=headers_bulk,data=data, timeout=5)
            if not sendBulk.text:
                # print("Empty response.")
                logging.info('====== -send_message_batch- ==============================================')
                logging.error('============ Phone: '+str(message_params['phone_number']))
                logging.error('Empty response.')
                logging.error('==========================================================================')
                for recipient in message_params['list_of_ids']:
                    r.mset({"recepient_" + str(recipient): "1"})
                # sendBulk.close()
            else:
                response_bulk = json.loads(sendBulk.text)
                logging.info('====== -send_message_batch- ==============================================')
                logging.info('============ Phone: '+str(message_params['phone_number']))
                logging.info(response_bulk)
                logging.info('==========================================================================')
                
                # print(headers_bulk)
                # print(response_bulk)
                # sendBulk.close()
                # if response_bulk["statusCode"] == "SC0000":
                # r.mset({"recepient_" + str(message_params["recepient_id"]): "1"})

                # if response_bulk["statusCode"] == "SC0000":
                #     # r.mset({"recepient_" + str(message_params["recepient_id"]): "1"})
                #     for recipient in message_params['list_of_ids']:
                #         r.mset({"recepient_" + str(recipient): "1"})
                
                for recipient in message_params['list_of_ids']:
                    r.mset({"recepient_" + str(recipient): "1"})
        except Exception as e:
        # requests.exceptions.Timeout:
            # r.mset({"recepient_" + str(message_params["recepient_id"]): "1"})
            # for recipient in message_params['list_of_ids']:
            #     r.mset({"recepientFailed_" + str(recipient): "1"})

            logging.error(e)
        
    except Exception as e:
        logging.error(e)


@app.task
def init_subscribe_manage(body):
    log_file = datetime.datetime.now().strftime('%d_%m_%Y')
    logging.basicConfig(format='%(asctime)s :: %(levelname)s :: %(message)s', filename='/opt/tasks_log_' + str(log_file) + '.log', filemode='a', level=logging.DEBUG)
    requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
    try:
        req_params = json.loads(body)
        Url = 'https://dsvc.safaricom.com:9480/api/public/SDP/activate';
        r = redis.Redis()

        bearer_text = str(r.get('bearer_text').decode())

        headers = {
            'Content-Type': 'application/json',
            'X-Requested-With': 'XMLHttpRequest',
            'X-Authorization':  bearer_text
        }

        data = {
                "requestId": req_params["identifier"], 
                "channel": "APIGW",
                "operation": "ACTIVATE",
                "requestParam": {
                    "data": [{"name": "Language", "value": 1},
                            {"name": "Msisdn", "value": req_params["phone_number"]},
                            {"name": "OfferCode", "value": req_params["offercode"]},
                            {"name": "CpId", "value": "50"}]
                }
                
            }
        
        data = json.dumps(data)
        # print(data)
        try:
            activateReq = requests.post(Url, headers=headers,data=data, timeout=5)
            if not activateReq.text:
                # print("Empty response.")
                logging.info('====== -init_subscribe_manage- ==============================================')
                logging.error('============ Phone: '+str(req_params['phone_number']))
                logging.error('Empty response.')
                logging.error('==========================================================================')
                
            else:
                response_req = json.loads(activateReq.text)
                logging.info('====== -init_subscribe_manage- ==============================================')
                logging.info('============ Phone: '+str(req_params['phone_number']))
                logging.info(response_req)
                logging.info('==========================================================================')
                
        except Exception as e:
            logging.error(e)
        
    except Exception as e:
        logging.error(e)


@app.task
def bill_subscribe_manage(body):
    log_file = datetime.datetime.now().strftime('%d_%m_%Y')
    logging.basicConfig(format='%(asctime)s :: %(levelname)s :: %(message)s', filename='/opt/tasks_log_' + str(log_file) + '.log', filemode='a', level=logging.DEBUG)
    requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
    try:
        req_params = json.loads(body)
        Url = 'https://dsvc.safaricom.com:9480/api/public/SDP/paymentRequest';
        r = redis.Redis()

        bearer_text = str(r.get('bearer_text').decode())

        headers = {
            'Content-Type': 'application/json',
            'X-Requested-With': 'XMLHttpRequest',
            'X-Authorization':  bearer_text
        }

        data = {
                "requestId": req_params["phone_number"], 
                "channel": "APIGW",
                "operation": "Payment",
                "requestParam": {
                    "data": [{"name": "Msisdn", "value": req_params["phone_number"]},
                            {"name": "OfferCode", "value": req_params["offercode"]},
                            {"name": "ChargeAmount", "value": req_params["chargeAmount"]}, 
                            {"name": "CpId", "value": "50"}]
                }
                
            }
        
        data = json.dumps(data)
        # print(data)
        try:
            paymentReq = requests.post(Url, headers=headers,data=data, timeout=5)
            if not paymentReq.text:
                # print("Empty response.")
                logging.info('====== -bill_subscribe_manage- ==============================================')
                logging.error('============ Phone: '+str(req_params['phone_number']))
                logging.error('Empty response.')
                logging.error('==========================================================================')
                
            else:
                response_req = json.loads(paymentReq.text)
                logging.info('====== -bill_subscribe_manage- ==============================================')
                logging.info('============ Phone: '+str(req_params['phone_number']))
                logging.info(response_req)
                logging.info('==========================================================================')
                
        except Exception as e:
            logging.error(e)
        
    except Exception as e:
        logging.error(e)


