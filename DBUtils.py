import json
from datetime import datetime
import psycopg2


def register_user(content) -> bool:
    id_num = content["user_id"]
    first_name = content["first_name"]
    last_name = content["last_name"]
    address = content["address"]
    json_object = {"id_num": id_num, "first_name": first_name, "last_name": last_name, "address": address}

    query = "INSERT INTO users(id_card_number, first_name, last_name, address) VALUES(%(id_num)s, %(first_name)s, " \
            "%(last_name)s, %(address)s) "

    success = execute_query(query, json_object)

    return success


def create_tables():
    create_users = "CREATE TABLE users (id_card_number INTEGER PRIMARY KEY, first_name VARCHAR(255) NOT NULL, " \
                   "last_name " \
                   "VARCHAR(255) NOT NULL, address VARCHAR(255) NOT NULL)"

    create_messages = "CREATE TABLE messages (message_id SERIAL PRIMARY KEY, sender INTEGER NOT NULL, receiver INTEGER " \
                      "NOT NULL, subject VARCHAR(255) NOT NULL, message_content VARCHAR(255), message_time TIMESTAMP " \
                      "NOT NULL, was_read BOOLEAN NOT NULL) "
    execute_query(create_users)


def clean_all_tables():
    execute_query("Delete from users; \n" + "Delete from messages")


def delete_messages_table():
    execute_query("Drop Table messages")


def print_tables():
    query = "SELECT * from messages"
    query2 = "SELECT * from users"

    conn = create_connection()
    cur = conn.cursor()
    cur.execute(query)
    table_data = cur.fetchall()
    print(table_data)

    cur.execute(query2)
    table_data = cur.fetchall()
    print(table_data)


def create_connection():
    try:
        conn = psycopg2.connect(dbname='daeidsaa851snt', user='mjcsasrflbmaiu',
                                password='3a305423fb4d06be08fa7c59e7966cf039038707212b28cf5a393fd7f77e178a',
                                host='ec2-52-208-175-161.eu-west-1.compute.amazonaws.com', port='5432',
                                sslmode='require')
    finally:
        return conn


def execute_query(query, json_object=None) -> bool:
    conn = None
    try:
        conn = create_connection()
        cur = conn.cursor()

        if json_object is None:
            cur.execute(query)
        else:
            cur.execute(query, json_object)

        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        return False
    finally:
        if conn is not None:
            conn.close()
    return True


def insert_message(content):
    sender_id = content["sender_id"]
    receiver_id = content["receiver_id"]
    subject = content["subject"]
    message_content = content["message_content"]
    message_time = datetime.now()
    was_read = False

    if validate_user(sender_id) is False or validate_user(receiver_id) is False:
        return json.dumps({"No such User": False}), 404

    query = "INSERT INTO messages(sender, receiver, subject, message_content, message_time, was_read) VALUES(%(" \
            "sender_id)s, %(receiver_id)s, %(subject)s, %(message_content)s, %(message_time)s, %(was_read)s)"

    json_object = {"sender_id": sender_id, "receiver_id": receiver_id, "subject": subject,
                   "message_content": message_content, "message_time": message_time, "was_read": was_read}
    execute_query(query, json_object)

    return json.dumps({"success": True}), 201


def delete_a_message(message_id):
    if validate_user(message_id) is False:
        return json.dumps({"No such Message": False}), 404
    query = "DELETE from messages WHERE message_id = %s"
    execute_query(query, (message_id,))


def read_a_message(content):
    message_id = content["message_id"]
    if validate_user(message_id) is False:
        return json.dumps({"No such Message": False}), 404
    query = "UPDATE messages SET was_read = TRUE WHERE message_id = %s"
    execute_query(query, (message_id,))


def get_messages_by_user_id(user_id, query):
    conn = create_connection()
    cur = conn.cursor()
    cur.execute(query, (user_id,))
    message_data = cur.fetchall()
    return message_data


def validate_user(user_id) -> bool:
    query = "SELECT 1 from users where id_card_number = %s"
    conn = create_connection()
    cur = conn.cursor()
    cur.execute(query, (user_id,))
    result = cur.fetchall()
    return len(result) > 0


def validate_message(message_id) -> bool:
    query = "SELECT 1 from messages where message_id = %s"
    conn = create_connection()
    cur = conn.cursor()
    cur.execute(query, (message_id,))
    result = cur.fetchall()
    return len(result) > 0