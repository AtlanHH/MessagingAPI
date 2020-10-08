from flask import Flask, json, request
from DBUtils import *
from datetime import datetime

app = Flask(__name__)


@app.route('/register', methods=['POST'])
def register():
    content = request.json
    result = register_user(content)

    if not result:
        return json.dumps({"user id already registered": False}), 404

    return json.dumps({"created": True}), 201


@app.route('/message/send', methods=['POST'])
def send_message():
    content = request.json
    print(content)
    return insert_message(content)


@app.route('/message/read', methods=['PUT'])
def read_message():
    content = request.json
    read_a_message(content)
    return json.dumps({"success": True}), 201


@app.route('/message/get/all', methods=['GET'])
def get_all_messages():
    args = request.args
    uid = args["user_id"]

    if validate_user(uid) is False:
        return json.dumps({"No such User": False}), 404

    if len(args) > 1:
        query_sent = "SELECT * FROM messages WHERE sender = %s ORDER BY message_time"
        sent_dict = get_messages_by_user_id(uid, query_sent)
        query_received = "SELECT * FROM messages WHERE receiver = %s ORDER BY message_time"
        received_dict = get_messages_by_user_id(uid, query_received)
        dict_all = sent_dict + received_dict
    else:
        query_received = "SELECT * FROM messages WHERE receiver = %s AND was_read = FALSE ORDER BY message_time"
        dict_all = get_messages_by_user_id(uid, query_received)

    return json.dumps(dict_all, default=date_converter), 200


def date_converter(o):
    if isinstance(o, datetime):
        return o.__str__()


@app.route('/message/delete', methods=['DELETE'])
def delete_message():
    args = request.args
    uid = args["message_id"]
    delete_a_message(uid)
    return json.dumps({"success": True}), 201


if __name__ == '__main__':
    app.run()