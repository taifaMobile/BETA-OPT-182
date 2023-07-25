import redis
import pymysql
import codecs


if __name__ == '__main__':
    while True:
        r = redis.Redis()
        keys = r.keys('*')
        # print(keys)
        if keys :
            keys_string = [key.decode() for key in keys]
            keys_string_delete = keys_string
            # print(keys_string)
            connection = pymysql.connect(
                host="",
                user="",
                password="",
                db="",
                charset='utf8mb4',
                port=,
                cursorclass=pymysql.cursors.DictCursor
            )
            # keys_string = map(str, keys_string)
            # keys_string = map(lambda x: x.replace('recepient_:', ''), keys_string)
            # print(type(keys_string))
            keys_string.remove('bearer_text')
            keys_string_delete = keys_string
            keys_string = [s.replace("recepient_", "") for s in keys_string]
            # failed_keys_string = [s.replace("recepientFailed_", "") for s in keys_string]

            # print(keys_string)
            if keys_string:
                try:
                    cursor = connection.cursor()
                        #Insert into recepients record
                    list_of_ids = tuple(keys_string)
                    if len(list_of_ids) == 1:
                        list_of_ids =  str(keys_string[0])
                        sql = "INSERT IGNORE INTO recepients_completed (recepient_id, message_id,phone_number,status_id,is_sent,is_picked,message_params) SELECT  recepient_id, message_id,phone_number,status_id,is_sent,is_picked,message_params FROM recepients where recepient_id = {};".format(
                            list_of_ids)
                    else:
                        sql = "INSERT IGNORE INTO recepients_completed (recepient_id, message_id,phone_number,status_id,is_sent,is_picked,message_params) SELECT  recepient_id, message_id,phone_number,status_id,is_sent,is_picked,message_params FROM recepients where recepient_id IN {};".format(
                            list_of_ids)
                    # print(list_of_ids)
                    # breakpoint()
                    cursor.execute(sql)
                    connection.commit()
                    # print(sql)

                    # update total number of messages processed
                    if len(tuple(keys_string)) == 1:
                        list_of_ids =  str(keys_string[0])
                        sql_update_count = "SELECT count(message_id) as total_updated, message_id FROM recepients WHERE recepient_id = {} GROUP BY message_id;".format(list_of_ids)
                    else:
                        sql_update_count = "SELECT count(message_id) as total_updated, message_id FROM recepients WHERE recepient_id IN {} GROUP BY message_id;".format(list_of_ids)
                    # print(sql_update_count)
                    cursor.execute(sql_update_count)
                    list_messages = cursor.fetchall()
                    for message_content in list_messages:
                        total_updated = message_content["total_updated"]
                        message_id = message_content["message_id"]
                        sql_update_messages = ("UPDATE messages SET sending_progress = (SELECT count(message_id) FROM recepients_completed WHERE message_id = %s GROUP BY message_id) WHERE message_id = %s " % (message_id, message_id))
                        # print(sql_update_messages)
                        cursor.execute(sql_update_messages)
                        connection.commit()

                        sql_update_messages_finished = ("UPDATE messages SET status_id = 2,is_finished = 1 WHERE message_id = {} AND sending_progress >= total".format(message_id) )
                        # print(sql_update_messages_finished)
                        cursor.execute(sql_update_messages_finished)
                        connection.commit()
                    if len(tuple(keys_string)) == 1:
                        list_of_ids =  str(keys_string[0])
                        sql_delete = "DELETE FROM recepients where recepient_id = {};".format(list_of_ids)

                    else:
                        sql_delete = "DELETE FROM recepients where recepient_id IN {};".format(tuple(keys_string))
                    # Delete Record and insert into recepients record
                    # print(sql_delete)
                    cursor.execute(sql_delete)
                    connection.commit()
                finally:
                    connection.close()
                # print(keys_string_delete)
                r.delete(*keys_string_delete)

            # if failed_keys_string:
            #     try:
            #         cursor = connection.cursor()
            #             #Insert into recepients record
            #         list_of_ids = tuple(failed_keys_string)
            #         if len(list_of_ids) == 1:
            #             list_of_ids =  str(failed_keys_string[0])
            #             sql = "UPDATE recepients SET status_id=0, is_sent=0, is_picked=0 where recepient_id = {};".format(
            #                 list_of_ids)
            #         else:
            #             sql = "UPDATE recepients SET status_id=0, is_sent=0, is_picked=0 where recepient_id IN {};".format(
            #                 list_of_ids)
            #         # print(list_of_ids)
            #         # breakpoint()
            #         cursor.execute(sql)
            #         connection.commit()
            #         # print(sql)

            #         # update total number of messages processed
            #     finally:
            #         connection.close()
            #     # print(keys_string_delete)
            #     r.delete(*keys_string_delete)