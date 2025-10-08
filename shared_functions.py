from datetime import datetime, timedelta
import hashlib
import json
import random
import sqlite3
import time
from click import echo
import redis
import re
import pytz
import requests
from ServerStatistics import ServerStatistics


def execute_query_to_queue_database(cursor, query, params=None):
    start_time = time.time()
    if params:
        cursor.execute(query, params)
    else:
        cursor.execute(query)
    end_time = time.time()
    execution_time = end_time - start_time
    if execution_time > 1:
        print(f"Query execution time: {execution_time:.6f} seconds")
        print(f"Query: {query}")
    return cursor

def execute_query_to_main_database(cursor, query, params=None):
    query = convert_sqlite_to_postgres(query)
    start_time = time.time()
    if params:
        cursor.execute(query, params)
    else:
        cursor.execute(query)
    if "update" in query.lower() or "insert" in query.lower() or "delete" in query.lower():
        cursor.connection.commit()
    end_time = time.time()
    execution_time = end_time - start_time
    if execution_time > 1:
        custom_log(f"Query execution time: {execution_time:.6f} seconds")
        custom_log(f"Query: {query}")
    return cursor

def convert_sqlite_to_postgres(query):
    # Convert SQLite's '?' to PostgreSQL's '%s'
    # Assumes that '?' is only used for placeholders, not as part of the query text
    return re.sub(r'\?', '%s', query)

def custom_log(value):
    iran_tz = pytz.timezone('Asia/Tehran')
    iran_time = datetime.now(iran_tz)
    print(f"{iran_time}       {value}")
    with open('database.log', 'a') as file:
        file.write(value+'.\n')

def generate_unique_string(unique_per_seconds):
    current_time = int(time.time())

    current_time -= current_time % unique_per_seconds

    time_str = str(current_time).encode('utf-8')

    sha256_hash = hashlib.sha256()

    sha256_hash.update(time_str)

    hex_digest = sha256_hash.hexdigest()[:6]

    return hex_digest

def md5_with_salt(value, salt):
    value_with_salt = str(value) + str(salt)
    hashed_value = hashlib.md5(value_with_salt.encode()).hexdigest()
    return hashed_value

def convert_digits_to_english(text):
    """
    Convert Persian/Arabic digits to English digits.
    Persian digits: ۰۱۲۳۴۵۶۷۸۹
    Arabic digits: ٠١٢٣٤٥٦٧٨٩
    English digits: 0123456789
    """
    if not text:
        return text
    
    persian_digits = '۰۱۲۳۴۵۶۷۸۹'
    english_digits = '0123456789'
    
    arabic_digits = '٠١٢٣٤٥٦٧٨٩'
    
    for i, persian_digit in enumerate(persian_digits):
        text = text.replace(persian_digit, english_digits[i])
    
    for i, arabic_digit in enumerate(arabic_digits):
        text = text.replace(arabic_digit, english_digits[i])
    
    return text

def normalize_digit_text(digit_string):
    """
    Normalize digit_string by converting non-English digits to English digits
    and ensuring it contains only digits.
    """
    if not digit_string:
        return digit_string
    
    normalized_digits = convert_digits_to_english(str(digit_string))
    
    normalized_digits = ''.join(filter(str.isdigit, normalized_digits))
    
    return normalized_digits

def create_and_store_vpn_client(db, cursor, should_be_lunched_urgent, local_inbound_id, inbound_id_in_vpn_server, config_id, user_email, user_password, user_id, user_partner_id, server_url, server_domain, server_id, vpn_protocol):
    shared_add_client(server_id, should_be_lunched_urgent, user_email,
                      user_password, inbound_id_in_vpn_server, "", "true", "0", "0", "0", vpn_protocol)
    cursor = execute_query_to_main_database(cursor,
                           'SELECT * FROM configs WHERE _id = ?', (config_id,))
    config = cursor.fetchone()
    if config:
        config_body = config['_config_body']
        config_body = config_body.replace(
            '%mail%', user_email)
        config_body = config_body.replace(
            '%password%', user_password)
        config_body = config_body.replace(
            "%main%", server_domain)
        cursor = execute_query_to_main_database(cursor, 'INSERT INTO vpn_cons (user_id, inbound_id, _connection, server_id, partner_id) VALUES (?, ?, ?, ?, ?)', (
            user_id, local_inbound_id, config_body, server_id, user_partner_id))
        db.commit()
        cursor = execute_query_to_main_database(cursor, "SELECT max(_id) as id FROM vpn_cons")
        vpn_cons_id = cursor.fetchone()['id']
        cursor = execute_query_to_main_database(cursor,
                               "SELECT * FROM server_tags WHERE server_id = ?", (server_id,))
        server_tags = cursor.fetchall()
        for server_tag in server_tags:
            cursor = execute_query_to_main_database(cursor, "INSERT INTO vpn_con_tags (vpn_con_id, server_tag_id, user_id, server_id) VALUES (?,?,?,?)", (
                vpn_cons_id, server_tag['_id'], user_id, server_id))
            db.commit()

def distribute_vpn_servers_to_user(db, servers_collection, user_id, should_be_lunched_urgent=True):
    testers = ['', '']
    try:
        cursor = db.cursor()
        user = get_user(cursor, user_id)
        if user:
            servers = get_available_servers(cursor, user, user_id)
            if servers:
                partner = get_partner(cursor, user)
                added_servers = get_added_servers_count(cursor, user)
                max_server_per_user = get_max_server_per_user(cursor,
                                                              partner, user, testers, servers_collection)
                server_tags_that_user_have = ""
                total_server_remained_to_add = len(servers) + 1
                random.shuffle(servers)
                for server in servers:
                    total_server_remained_to_add -= 1
                    if added_servers >= max_server_per_user:
                        break
                    if is_server_ok_to_add(servers_collection, server, user):
                        server_tags_array = server['_tags'].split(",")
                        if is_server_tags_ok_to_add(server_tags_array, server_tags_that_user_have, partner, max_server_per_user, added_servers, total_server_remained_to_add):
                            local_inbound = get_local_inbound(cursor, server)
                            if local_inbound:
                                vpn_protocol = server['_vpn_protocol']
                                create_and_store_vpn_client(db, cursor, should_be_lunched_urgent, local_inbound['_id'], local_inbound['inbound_id_in_vpn_server'], local_inbound[
                                                            'config_id'], user['_user_email'], user['_user_password'], user['_id'], partner['_id'], server['_url'], server['_domain'], server['_id'], vpn_protocol)
                                server_tags_that_user_have = update_server_tags(
                                    server_tags_array, server_tags_that_user_have)
                                added_servers += 1
                if added_servers > 0:
                    update_user_connection_hash(db, user_id)
    except Exception as e:
        print(f"Exception is : {str(e)}")

def get_user(cursor, user_id):
    cursor = execute_query_to_main_database(
        cursor, "SELECT * FROM users WHERE _id = ?", (user_id,))
    return cursor.fetchone()

def get_available_servers(cursor, user, user_id):
    query = "SELECT * FROM servers WHERE partner_id = ? AND _state > 0 and _id NOT IN (SELECT server_id FROM vpn_cons WHERE user_id = ? AND server_id IS NOT NULL)"
    cursor = execute_query_to_main_database(cursor, query,
                           ((user['provider_id'] if user['provider_id'] > 0 else user['partner_id']), user_id,))
    return cursor.fetchall()

def get_partner(cursor, user):
    cursor = execute_query_to_main_database(cursor, "SELECT * FROM partners WHERE _id = ?",
                           (user['partner_id'],))
    return cursor.fetchone()

def get_added_servers_count(cursor, user):
    query = "SELECT * FROM vpn_cons WHERE user_id = ? AND server_id IS NOT NULL"
    cursor = execute_query_to_main_database(cursor, query, (user['_id'],))
    added_servers = cursor.fetchall()
    return len(added_servers) if added_servers else 0

def get_max_server_per_user(cursor, partner, user, testers, servers_collection):
    if "tun___-1" in partner['_tags']:
        cursor = execute_query_to_main_database(cursor,
                               "SELECT * FROM servers WHERE partner_id = ?", (partner['_id'],))
        servers = cursor.fetchall()
        count = 0
        for server in servers:
            if "tun___" not in server['_tags']:
                stats = server['_statistics']
                if stats is None:
                    serStats = ServerStatistics(servers_collection, 1)
                    stats = serStats.get_statistics(server)            
                for stat in stats['operator_stats']:
                    if stat['operator'] == user['_simcard_carear'] and stat['records_count'] > 10:
                        success_rate = 100 * \
                            (stat['records_count'] - stat['failures']) / \
                            stat['records_count']
                        if success_rate > 60:
                            count += 1
                            break
        return count
    return partner['_max_server_per_user'] if user['_customer_support_number'] not in testers else 6

def is_server_ok_to_add(servers_collection, server, user):
    time_now = int(datetime.now().timestamp() * 1000)
    time_1_hours_ago = int(
        (datetime.now() - timedelta(minutes=10)).timestamp() * 1000)
    user_statistics_for_this_server = servers_collection.find({
        'server_id': str(server['_id']),
        'user_id': int(user['_id']),
        'utc_time': {
            '$gte': time_1_hours_ago,
            '$lte': time_now
        }
    }).limit(1).sort('time', -1)
    user_statistics_list = list(user_statistics_for_this_server)
    if user_statistics_list and len(user_statistics_list) > 0 and user_statistics_list[0]['real_ping'] == -1:
        return False
    stats = server['_statistics']
    if stats is None:
        serStats = ServerStatistics(servers_collection, 1)
        stats = serStats.get_statistics(server)
        
    for stat in stats['operator_stats']:
        if stat['operator'] == user['_simcard_carear'] and stat['records_count'] > 10:
            if stat['avg_real_ping'] > 3000 or stat['avg_tcp_ping'] > 3000 or amplify(100 * (stat['records_count'] - stat['failures']) / stat['records_count']) < random.randint(0, 100):
                return False
    return True

def is_server_tags_ok_to_add(server_tags_array, server_tags_that_user_have, partner, max_server_per_user, added_servers, total_server_remained_to_add):
    if "tun___-1" in partner['_tags'] and "tun___" in server_tags_array:
        return False

    for server_tag in server_tags_array:
        if "___" in server_tag:
            server_tag_name, server_tag_number = server_tag.split("___")
            server_tag_number = float(server_tag_number)
            if server_tag_name == "tun" and server_tag_number == 0:
                return False
            list_of_server_tags_that_user_have = server_tags_that_user_have.split(
                ',')
            user_has_this_tag_name_count = list_of_server_tags_that_user_have.count(
                server_tag_name)
            if isinstance(server_tag_number, float):
                if (not server_tag_number.is_integer() and (user_has_this_tag_name_count + 1) > (server_tag_number * partner['_max_server_per_user'])) or (server_tag_number.is_integer() and (user_has_this_tag_name_count + 1) > server_tag_number):
                    if total_server_remained_to_add > (max_server_per_user - added_servers):
                        return False
    return True

def get_local_inbound(cursor, server):
    cursor = execute_query_to_main_database(cursor,
                           "SELECT * FROM local_inbounds WHERE server_id = ?", (server['_id'],))
    return cursor.fetchone()

def update_server_tags(server_tags_array, server_tags_that_user_have):
    for server_tag in server_tags_array:
        server_tag_name = str(server_tag.split("___")[0])
        server_tags_that_user_have += f"{server_tag_name},"
    return server_tags_that_user_have

def update_user_connection_hash(db, user_id):
    cursor = db.cursor()
    cursor = execute_query_to_main_database(cursor,
                           'UPDATE users SET _connection_hash = ? WHERE _id = ?', ('', user_id,))
    cursor.connection.commit()

def delete_server_from_main_database(db, server_id, partner_id=0):
    cursor = db.cursor()
    cursor = execute_query_to_main_database(cursor, 'SELECT * FROM servers WHERE _id = ? AND partner_id = ?', (server_id, partner_id,))

    server = cursor.fetchone()
    if not server:
        return ("server not found", 400)
    return delete_vpn_connections(db, "by_server", server_id=server['_id'])

def amplify(x):
    if x < 50:
        return 50 * (x / 50)**2
    elif x > 50:
        return 100 - 50 * ((100 - x) / 50)**2
    else:
        return 50

def delete_vpn_connections(db, operation, server_id=None, user_id=0, inbound_id=0, uuid='', email=''):
    cursor = db.cursor()
    if operation == "by_user":
        cursor = execute_query_to_main_database(cursor, "SELECT * FROM users WHERE _id  = ?",
                               (user_id,))
        user = cursor.fetchone()
        if user:
            cursor = execute_query_to_main_database(cursor, "SELECT * FROM vpn_cons WHERE user_id = ?",(user['_id'],))
            vpn_cons = cursor.fetchall()
            for vpn_con in vpn_cons:
                if vpn_con['server_id'] > 0:
                    cursor = execute_query_to_main_database(cursor,
                                        'SELECT * FROM servers WHERE _id = ?', (server_id,))
                    server = cursor.fetchone()
                    if server:
                        vpn_protocol = server['_vpn_protocol']
                        cursor = execute_query_to_main_database(cursor, "SELECT * FROM local_inbounds WHERE _id  = ?",(vpn_con['inbound_id'],))
                        inbound = cursor.fetchone()
                        if inbound:
                            shared_delete_client(
                                vpn_con['server_id'], False, inbound['inbound_id_in_vpn_server'], user['_user_password'], vpn_protocol, user['_user_email'])
                else:
                    cursor = execute_query_to_main_database(cursor,
                                   'UPDATE standalone_configs SET _user_count = _user_count - 1 WHERE _id = ?', (vpn_con['standalone_config_id'],))
                    db.commit()
            cursor = execute_query_to_main_database(cursor,
                                            'DELETE FROM vpn_con_tags WHERE _id IN (SELECT _id FROM vpn_cons WHERE user_id = ? AND server_id IS NOT NULL)', (user['_id'],))
            db.commit()
            cursor = execute_query_to_main_database(cursor,
                                   'DELETE FROM vpn_cons WHERE user_id = ?', (user['_id'],))
            db.commit()
            cursor = execute_query_to_main_database(cursor,
                                   'UPDATE users SET _vpn_is_active = 0, _total = 0, _day = -1, _upload = 0, _download = 0 , _delta_usage = 0, _connection_hash = ? WHERE _id = ?', ('', user['_id'],))
            db.commit()
            
    elif operation == "by_server":
        try:
            cursor = execute_query_to_main_database(cursor,
                                   '''SELECT _id FROM local_inbounds WHERE server_id = ? ''', (server_id,))
            local_inbounds_to_delete = cursor.fetchall()
            for local_inbound_to_delete in local_inbounds_to_delete:
                cursor = execute_query_to_main_database(cursor, '''SELECT * FROM users
                        LEFT JOIN vpn_cons ON vpn_cons.user_id = users._id
                        WHERE vpn_cons.inbound_id  = ?''', (local_inbound_to_delete['_id'],))
                users_who_should_update_their_total = cursor.fetchall()
                for (mUser) in users_who_should_update_their_total:
                    new_total = mUser['_total'] - float(mUser['_usage'])
                    new_upload = mUser['_upload'] - mUser['_client_upload']
                    new_download = mUser['_download'] - \
                        mUser['_client_download']
                    cursor = execute_query_to_main_database(cursor,
                                           'UPDATE users SET  _connection_hash = ?, _total = ?, _upload = ?, _download = ? WHERE _id = ?', ('', new_total, new_upload, new_download, mUser['_id'],))
                    db.commit()

            cursor = execute_query_to_main_database(cursor,
                                       'DELETE FROM vpn_con_tags WHERE server_id = ?', (server_id,))
            db.commit()
            cursor = execute_query_to_main_database(cursor,
                                    'DELETE FROM vpn_cons WHERE server_id = ?', (server_id,))
            db.commit()
            cursor = execute_query_to_main_database(cursor,
                                    'DELETE FROM server_tags WHERE server_id = ?', (server_id,))
            db.commit()
            cursor = execute_query_to_main_database(cursor,
                                    'DELETE FROM local_inbounds WHERE _id = ?', (local_inbound_to_delete['_id'],))
            db.commit()
            cursor = execute_query_to_main_database(cursor,
                                   'DELETE FROM configs WHERE server_id = ?', (server_id,))
            db.commit()
            cursor = execute_query_to_main_database(cursor,
                                   'DELETE FROM servers WHERE _id = ?', (server_id,))
            db.commit()
            
            queue_conn = sqlite3.connect("client-management-queue.db")
            queue_cursor = queue_conn.cursor()
            queue_cursor = execute_query_to_queue_database(queue_cursor,
                                         'DELETE FROM important_tasks_queue_table WHERE server_id = ?',
                                         (server_id,))
            queue_conn.commit()
            queue_cursor = execute_query_to_queue_database(queue_cursor,
                                         'DELETE FROM queue_table WHERE server_id = ?',
                                         (server_id,))
            queue_conn.commit()
            queue_cursor = execute_query_to_queue_database(queue_cursor,
                                         'DELETE FROM servers WHERE server_id = ?',
                                         (server_id,))
            queue_conn.commit()
            queue_conn.close()
            return ("Server deleted successfully", 200)
        except Exception as e:
            print(str(e))
            return (str(e), 500)
    elif operation == "just_connection_server":
        cursor = execute_query_to_main_database(cursor,
                               'SELECT * FROM servers WHERE _id = ?', (server_id,))
        server = cursor.fetchone()
        if server:
            vpn_protocol = server['_vpn_protocol']
            cursor = execute_query_to_main_database(cursor,
                                'SELECT * FROM vpn_cons WHERE server_id = ? AND user_id = ?', (server_id, user_id,))
            
            vpn_con = cursor.fetchone()
            if vpn_con:
                cursor = execute_query_to_main_database(cursor,
                                    'DELETE FROM vpn_con_tags WHERE vpn_con_id = ?', (vpn_con['_id'],))
                db.commit()
                cursor = execute_query_to_main_database(cursor,
                                    'DELETE FROM vpn_cons WHERE _id = ?', (vpn_con['_id'],))
                db.commit()
                cursor = execute_query_to_main_database(cursor,
                                    'UPDATE users SET  _connection_hash = ?, _total = _total - ?, _upload = _upload - ?, _download = _download - ? WHERE _id = ?', ('', vpn_con['_usage'], vpn_con['_client_upload'], vpn_con['_client_download'], user_id,))
                db.commit()
                cursor = execute_query_to_main_database(cursor, "SELECT * FROM users WHERE _id  = ?",
                                    (user_id,))
                user = cursor.fetchone()
                shared_delete_client(server_id, False, vpn_con['inbound_id'],
                                    user['_user_password'], vpn_protocol, user['_user_email'])
    elif operation == "just_connection_std_cnf":
        cursor = execute_query_to_main_database(cursor,
                               'SELECT * FROM vpn_cons WHERE standalone_config_id = ? AND user_id = ?', (server_id, user_id,))
        vpn_con = cursor.fetchone()
        if vpn_con:
            cursor = execute_query_to_main_database(cursor,
                                   'DELETE FROM vpn_cons WHERE _id = ?', (vpn_con['_id'],))
            db.commit()
            cursor = execute_query_to_main_database(cursor,
                                   'UPDATE users SET  _connection_hash = ? WHERE _id = ?', ('', user_id,))
            db.commit()
            cursor = execute_query_to_main_database(cursor,
                                   'UPDATE standalone_configs SET _user_count = _user_count - 1 WHERE _id = ?', (server_id,))
            db.commit()

async def shared_write_to_kafka_queue(is_important_task, operation, server_id, email, uuid, inbound_id, client_flow, stringLowerCaseBooleanExpressinForEnable, limitIp, totalGB, expiryTime, vpn_protocol):
    """Write a task to Kafka queue instead of SQLite database"""
    try:
        redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        redis_key = f"server:{server_id}:task_count"
        task_data = redis_client.hgetall(redis_key)
        redis_client.close()
        if task_data:
            total_tasks = int(task_data.get('total', 0))
            if total_tasks > 500:
                print(f"Server {server_id} has {total_tasks} pending tasks, skipping new task to prevent queue overflow")
                return False
    except Exception as e:
        redis_client.close()
        print(f"Failed to check task count for server {server_id}: {e}")

    topic = "client-management-urgent-queue" if is_important_task else "client-management-queue"

    message = {
        'operation': operation,
        'server_id': server_id,
        'email': email,
        'uuid': uuid,
        'inbound_id': inbound_id,
        'client_flow': client_flow,
        'stringLowerCaseBooleanExpressinForEnable': stringLowerCaseBooleanExpressinForEnable,
        'limitIp': limitIp,
        'totalGB': totalGB,
        'expiryTime': expiryTime,
        'vpn_protocol': vpn_protocol,
        'timestamp': time.time(),
        'state': 0,  # 0 = pending, 1 = processing
        'proxy': ''  # Will be set by the worker
    }

    key = str(server_id)

    from kafka import KafkaProducer
    import json
    import asyncio

    loop = asyncio.get_event_loop()
    success = False
    try:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8'),
            linger_ms=10
        )
        future = producer.send(topic, value=message, key=key)
        result = await loop.run_in_executor(None, future.get, 10)
        if result:
            print(f"Task {operation} for server {server_id} added to {topic}")
            success = True
        else:
            print(f"Failed to add task {operation} for server {server_id} to {topic}")
            success = False
    except Exception as e:
        print(f"Kafka send error: {e}")
        success = False
    finally:
        try:
            producer.close()
        except Exception:
            pass
    return success

def shared_write_to_db(is_important_task, operation, server_id, email, uuid, inbound_id, client_flow, stringLowerCaseBooleanExpressinForEnable, limitIp, totalGB, expiryTime, vpn_protocol):
    """Legacy function - now calls the Kafka version"""
    import asyncio
    try:
        loop = asyncio.get_running_loop()
        task = loop.create_task(shared_write_to_kafka_queue(is_important_task, operation, server_id, email, uuid, inbound_id, client_flow, stringLowerCaseBooleanExpressinForEnable, limitIp, totalGB, expiryTime, vpn_protocol))
        return True
    except RuntimeError:
        return asyncio.run(shared_write_to_kafka_queue(is_important_task, operation, server_id, email, uuid, inbound_id, client_flow, stringLowerCaseBooleanExpressinForEnable, limitIp, totalGB, expiryTime, vpn_protocol))

def shared_delete_client(server_id, is_important_task, inbound_id, uuid, vpn_protocol, email = ""):
    try:
        shared_write_to_db(is_important_task, "delete", server_id, email,
                           uuid, inbound_id, "", "true", "0", "0", "0", vpn_protocol)
    except Exception as e:
        print(str(e))

def shared_add_client(server_id, is_important_task, email, uuid, inbound_id, client_flow, stringLowerCaseBooleanExpressinForEnable,
                      limitIp, totalGB, expiryTime, vpn_protocol, delete_if_exist=True):
    if delete_if_exist:
        operation = "add"
    else:
        operation = "add_if_not_exist"
    shared_write_to_db(is_important_task, operation,  server_id, email,
                       uuid, inbound_id, client_flow, stringLowerCaseBooleanExpressinForEnable, limitIp, totalGB, expiryTime, vpn_protocol)

def shared_change_server_state(server_id):
    operation = "change_state"
    shared_write_to_db(False, operation, server_id, "", "", 0, "", "", "", "", "", False)

def make_branch_new_link(channel, feature, campaign, stage, tags, type_val, data, branch_key_live ,proxies = None):
    url = 'https://api.branch.io/v1/url'

    payload = {
        "branch_key": branch_key_live,
        "channel": channel,
        "feature": feature,
        "campaign": campaign,
        "stage": stage,
        "tags": tags,
        "type": type_val,
        "data": data
    }

    headers = {'Content-Type': 'application/json'}
    if not proxies:
        response = requests.post(url, data=json.dumps(payload), headers=headers)
    else:
        response = requests.post(url, data=json.dumps(payload), headers=headers, proxies=proxies)

    if response.status_code == 200:
        result = response.json()
        return result.get('url')
    else:
        return None

def update_branch_link(link_id, channel=None, feature=None, campaign=None, stage=None, tags=None, type_val=None, data=None, branch_key_live=None):
    """
    Update an existing Branch.io link with new parameters.
    
    Args:
        link_id (str): The ID of the link to update
        channel (str, optional): The channel for the link
        feature (str, optional): The feature for the link
        campaign (str, optional): The campaign for the link
        stage (str, optional): The stage for the link
        tags (list, optional): List of tags for the link
        type_val (int, optional): The type value for the link
        data (dict, optional): Additional data for the link
        branch_key_live (str, optional): The Branch.io live key
        
    Returns:
        str: The updated link URL if successful, None otherwise
    """
    url = f'https://api.branch.io/v1/url/{link_id}'
    
    payload = {}
    if channel is not None:
        payload['channel'] = channel
    if feature is not None:
        payload['feature'] = feature
    if campaign is not None:
        payload['campaign'] = campaign
    if stage is not None:
        payload['stage'] = stage
    if tags is not None:
        payload['tags'] = tags
    if type_val is not None:
        payload['type'] = type_val
    if data is not None:
        payload['data'] = data
    if branch_key_live is not None:
        payload['branch_key'] = branch_key_live
        
    headers = {'Content-Type': 'application/json'}
    
    response = requests.put(url, data=json.dumps(payload), headers=headers)
    
    if response.status_code == 200:
        result = response.json()
        return result.get('url')
    else:
        return None
