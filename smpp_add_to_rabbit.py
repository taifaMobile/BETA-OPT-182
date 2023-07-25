"""Convert submit_sm to deliver_sm and reinject it back in rabbitmq
as coming from connector's CID same as submit_sm's uid
 
Design is diagrammed here: https://sketchboard.me/nAvXcoQqcbIt#/"""
 
import pickle
import logging
import uuid
import pika
import MySQLdb as mdb
from datetime import datetime
from jasmin.managers.content import DeliverSmContent
from jasmin.routing.Routables import RoutableDeliverSm
from jasmin.routing.jasminApi import Connector
from smpp.pdu.operations import DeliverSM
from jasmin.managers.content import DLRContentForSmpps
 
RABBITMQ_URL = 'amqp://guest:guest@localhost:5672/%2F'
 
def claim_msgid_for_uid(uid):
    try:
        with open('/var/tmp/%s' % uid, 'r+') as f:
            msgid = int(f.read()) + 1
            f.seek(0)
            f.truncate()
            f.write(str(msgid))
    except IOError:
        msgid = 1
        with open('/var/tmp/%s' % uid, 'w') as f:
            f.write('1')
 
    return str(msgid)
 
# Init logger
logger = logging.getLogger('routable_params')
if len(logger.handlers) != 1:
    hdlr = logging.FileHandler('/var/log/jasmin/routable_params.log')
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.DEBUG)
 
 
logger.debug("Received a routable from user %s", routable.user.uid)

routable.pdu.params['source_addr'] ='china-skyline'
routable.lockPduParam('source_addr')
 
submit_sm = routable.pdu
logger.debug("Got submit_sm: %s", submit_sm)
deliver_sm = DeliverSM(
    submit_sm.seqNum,
    service_type=submit_sm.params['service_type'],
    source_addr_ton=submit_sm.params['source_addr_ton'],
    source_addr_npi=submit_sm.params['source_addr_npi'],
    source_addr=submit_sm.params['source_addr'],
    dest_addr_ton=submit_sm.params['dest_addr_ton'],
    dest_addr_npi=submit_sm.params['dest_addr_npi'],
    destination_addr=submit_sm.params['destination_addr'],
    esm_class=submit_sm.params['esm_class'],
    protocol_id=submit_sm.params['protocol_id'],
    priority_flag=submit_sm.params['priority_flag'],
    registered_delivery=submit_sm.params['registered_delivery'],
    replace_if_present_flag=submit_sm.params['replace_if_present_flag'],
    data_coding=submit_sm.params['data_coding'],
    short_message=submit_sm.params['short_message'],
    sm_default_msg_id=submit_sm.params['sm_default_msg_id'])
logger.debug("Prepared a new deliver_sm: %s", deliver_sm)
 
# Prepare for deliver_sm injection
_routable = RoutableDeliverSm(deliver_sm, Connector(routable.user.uid))
content = DeliverSmContent(_routable, routable.user.uid, pickleProtocol=pickle.HIGHEST_PROTOCOL)
routing_key = 'deliver.sm.%s' % routable.user.uid
 
# Connecto RabbitMQ and publish deliver_sm
logger.info('Init pika and publish..')
connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
channel = connection.channel()
logger.debug('RabbitMQ channel ready, publishing now msgid %s ...', content.properties['message-id'])
channel.basic_publish(
    'messaging',
    routing_key,
    content.body,
    pika.BasicProperties(
        message_id=content.properties['message-id'],
        headers=content.properties['headers']))
logger.debug('Published deliver_sm to %s', routing_key)
 
# Explicitly return ESME_ROK
# This will bypass Jasmin's router as
# described in gh #589
smpp_status = 0
http_status = 200
logger.info("Returning smpp_status %s http_status %s", smpp_status, http_status)
 
# Returning a msgid with ESME_ROK
if routable.user.uid == 'entel':
    extra['message_id'] = claim_msgid_for_uid(routable.user.uid)
else:
    extra['message_id'] = str(uuid.uuid4())
 
# Send back a DLR
status = 'DELIVRD'
dlr_content = DLRContentForSmpps(
    status,
    extra['message_id'],
    routable.user.uid,
    submit_sm.params['source_addr'],
    submit_sm.params['destination_addr'],
    datetime.now(),
    str(submit_sm.params['dest_addr_ton']),
    str(submit_sm.params['dest_addr_npi']),
    str(submit_sm.params['source_addr_ton']),
    str(submit_sm.params['source_addr_npi']),
)
 
# Publish DLR
routing_key = 'dlr_thrower.smpps'
channel.basic_publish(
    'messaging',
    routing_key,
    dlr_content.body,
    pika.BasicProperties(
        message_id=dlr_content.properties['message-id'],
        headers=dlr_content.properties['headers']))
logger.debug("Published DLRContentForSmpps[%s] to [%s], having status = %s, short_message=%s",
            extra['message_id'], routing_key, status, submit_sm.params['short_message'])
            
msg_id=extra['message_id']
destination_addr = routable.pdu.params['destination_addr'].decode("utf-8")
short_message = routable.pdu.params['short_message'].decode("utf-8")

try:
    con = mdb.connect('localhost', 'tfm_sms_user', 'W9gGq!!%qTvWb2R', 'tm_prsp_db');
    cur = con.cursor()
 
    logger.info('inserting to recepients')
    logger.info(destination_addr)
    cur.execute("INSERT INTO recepients (message_id,phone_number,status_id,is_picked) VALUES (13,{},1,0)".format(destination_addr))
    logger.info('Succesfully inserted recepients params to db')
    con.commit()

    message_id = 'smpp-{}'.format(cur.lastrowid)

    logger.info('inserting {} to smpp log'.format(message_id))
    cur.execute(
        """INSERT INTO smpp_log (msg_id,message_id,source_addr,destination_addr,short_message) VALUES (%s,%s,%s,%s,%s) """,
        (msg_id,message_id, routable.pdu.params['source_addr'], destination_addr, short_message))
    logger.info('Succesfully inserted routable params to db')
    con.commit()
finally:
    if con:
        con.close()
 
# Tear down
connection.close()