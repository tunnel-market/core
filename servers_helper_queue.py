import asyncio
import random
import sqlite3
import aiohttp
from aiohttp_socks import ProxyConnector
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from shared_functions import execute_query_to_queue_database


def setup_retry_policy(session, retries, backoff_factor, status_forcelist):
    retry_strategy = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

class ServersHelperQueue:

    def get_db(self):
        db = sqlite3.connect("client-management-queue.db",
                             check_same_thread=False)
        db.execute('pragma journal_mode=wal')
        db.row_factory = sqlite3.Row
        return db

    def __init__(self, server_id, proxy):
        db = self.get_db()
        try:
            self.id = server_id
            self.proxy = proxy
            cursor = db.cursor()
            cursor = execute_query_to_queue_database(cursor,
                'SELECT * FROM servers WHERE server_id = ?', (server_id,))
            server = cursor.fetchone()
            if server:
                if server['_cookie'] is not None and server['_cookie'] != '':
                    self.set_attrs(server)
                else:
                    url = server['_url'] + '/login'
                    payload = {
                        'username': server['_api_user_name'],
                        'password': server['_api_password']
                    }
                    session = requests.Session()
                    session = setup_retry_policy(
                        session, retries=1, backoff_factor=0.3, status_forcelist=[500, 502, 503, 504])
                    response = session.post(url, data=payload, timeout=5)
                    # response = session.post(url, data=payload, timeout=5, proxies={"http": self.proxy, "https": self.proxy})

                    if response.status_code == 200:
                        cookie = response.headers.get('Set-Cookie')
                        self.cookie = cookie
                        cursor = execute_query_to_queue_database(cursor,
                            'UPDATE servers SET _cookie = ? WHERE server_id = ?', (cookie, server['server_id']))
                        db.commit()
                        cursor = execute_query_to_queue_database(cursor,
                            'SELECT * FROM servers WHERE server_id = ?', (server_id,))
                        created_server = cursor.fetchone()
                        self.set_attrs(created_server)
                    else:
                        self.set_attrs(False)
            else:
                self.set_attrs(False)
        except Exception as e:
            print(f'exception happnd in init: {str(e)}')
            self.set_attrs(False)

    def set_attrs(self, db_result):
        if db_result:
            self.id = db_result['server_id']
            self.url = db_result['_url']
            self.type = db_result['_type']
            self.cookie = db_result['_cookie']
            self.api_user_name = db_result["_api_user_name"]
            self.api_password = db_result['_api_password']
            if self.type == "2":
                self.base_for_api = "/panel/api/inbounds"
            else:
                self.base_for_api = "/xui/API/inbounds"
        else:
            self.cookie = 0

    async def is_valid_json(self, response):
        try:
            await response.json()
            return True
        except ValueError:
            return False

    async def queue_add_client_even_if_exist(self, email, uuid, inbound_id, client_flow, stringLowerCaseBooleanExpressinForEnable,
                                       limitIp, totalGB, expiryTime, vpn_protocol):
        try:
            if self.cookie == 0:
                return

            url = f'{self.url}{self.base_for_api }/getClientTraffics/{email}'

            headers = {
                "Accept": "application/json",
                "Cookie": self.cookie
            }
            connector = None
            if self.proxy:
                try:
                    connector = ProxyConnector.from_url(self.proxy)
                except ValueError:
                    raise ValueError(
                        "Invalid proxy type. Use 'http' or 'socks'.")
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, timeout=5) as response:
                    if response.status == 200:
                        if await self.is_valid_json(response):
                            data = await response.json()
                            if "obj" in data:
                                if data["obj"] is not None:
                                    await self.queue_dl_client(inbound_id, uuid, vpn_protocol ,email)
                    await asyncio.sleep(random.uniform(100, 300)/100)
                    return await self.add_queue_client(email, uuid, inbound_id, client_flow, stringLowerCaseBooleanExpressinForEnable, limitIp, totalGB, expiryTime, vpn_protocol)
        except Exception as e:
            print(str(e))

    async def queue_add_client_if_not_exist(self, email, uuid, inbound_id, client_flow, stringLowerCaseBooleanExpressinForEnable,
                                       limitIp, totalGB, expiryTime, vpn_protocol):
        try:
            if self.cookie == 0:
                return

            url = f'{self.url}{self.base_for_api }/getClientTraffics/{email}'

            headers = {
                "Accept": "application/json",
                "Cookie": self.cookie
            }
            connector = None
            if self.proxy:
                try:
                    connector = ProxyConnector.from_url(self.proxy)
                except ValueError:
                    raise ValueError(
                        "Invalid proxy type. Use 'http' or 'socks'.")
                    
            async with aiohttp.ClientSession() as session:
            # async with aiohttp.ClientSession(connector=connector) as session:
                async with session.get(url, headers=headers, timeout=5) as response:
                    if response.status == 200:
                        if await self.is_valid_json(response):
                            data = await response.json()
                            if "obj" in data:
                                if data["obj"] is not None:
                                    print("client exist")
                                    return True
                                
                    return await self.add_queue_client(email, uuid, inbound_id, client_flow, stringLowerCaseBooleanExpressinForEnable, limitIp, totalGB, expiryTime, vpn_protocol)
        except Exception as e:
            print(str(e))
    
    async def add_queue_client(self, email, uuid, inbound_id, client_flow, stringLowerCaseBooleanExpressinForEnable, limitIp, totalGB, expiryTime, vpn_protocol):
        try:
            if self.cookie == 0:
                return False

            url = self.url + self.base_for_api + '/addClient'

            if vpn_protocol == "trojan":    
                payload = {
                    "id": inbound_id,
                    "settings": f'{{"clients":[{{"password":"{uuid}","flow":"{client_flow}","alterId":0,"email":"{email}","limitIp":{limitIp},"totalGB":{totalGB},"expiryTime":{expiryTime},"enable":{stringLowerCaseBooleanExpressinForEnable},"tgId":"","subId":""}}]}}'
                }
            else:    
                payload = {
                    "id": inbound_id,
                    "settings": f'{{"clients":[{{"id":"{uuid}","flow":"{client_flow}","alterId":0,"email":"{email}","limitIp":{limitIp},"totalGB":{totalGB},"expiryTime":{expiryTime},"enable":{stringLowerCaseBooleanExpressinForEnable},"tgId":"","subId":""}}]}}'
                }
            headers = {
                "Accept": "application/json",
                "Cookie": self.cookie
            }

            connector = None
            if self.proxy:
                try:
                    connector = ProxyConnector.from_url(self.proxy)
                except ValueError:
                    raise ValueError(
                        "Invalid proxy type. Use 'http' or 'socks'.")
            async with aiohttp.ClientSession() as session:
            # async with aiohttp.ClientSession(connector=connector) as session:
                async with session.post(url, data=payload, headers=headers, timeout=5) as response:
                    if await self.is_valid_json(response):
                        data = await response.json()
                        if 'success' in data and data['success'] == True:
                            return True
                        else:
                            print(data)
                            return False
                    else:
                        print(f"add_queue_client failed response is not valid json")
                        print(response)
                        return False
        except Exception as e:
            print(f"add_queue_client Exception {str(e)}")
            return False


    async def queue_dl_client(self, inbound_id, uuid, vpn_protocol ,email=''):
        if email != '':
            await self.reset_clients_trafic(inbound_id, email, vpn_protocol)
        try:
            if self.cookie == 0:
                return False

            url = f'{self.url}{self.base_for_api}/{inbound_id}/delClient/{uuid}'

            headers = {
                "Accept": "application/json",
                "Cookie": self.cookie
            }
            if self.proxy:
                try:
                    connector = ProxyConnector.from_url(self.proxy)
                except ValueError:
                    raise ValueError(
                        "Invalid proxy type. Use 'http' or 'socks'.")
            else:
                connector = None
            async with aiohttp.ClientSession() as session:
            # async with aiohttp.ClientSession(connector=connector) as session:
                async with session.post(url, headers=headers, timeout=5) as response: 
                    if await self.is_valid_json(response):
                        data = await response.json()
                    if response.status == 200:
                        return True
                    else:
                        return False
        except Exception as e:
            print(
                f"exception happened in delete_client : {str(e)} , server_id is {self.id}")
            return False

    async def reset_clients_trafic(self, inbound_id, emial, vpn_protocol):
        try:
            if self.cookie == 0:
                return False

            url = f'{self.url}{self.base_for_api}/{inbound_id}/resetClientTraffic/{emial}'

            headers = {
                "Accept": "application/json",
                "Cookie": self.cookie
            }

            connector = None
            if self.proxy:
                try:
                    connector = ProxyConnector.from_url(self.proxy)
                except ValueError:
                    raise ValueError(
                        "Invalid proxy type. Use 'http' or 'socks'.")

            async with aiohttp.ClientSession() as session:
            # async with aiohttp.ClientSession(connector=connector) as session:
                async with session.post(url, headers=headers, timeout=5) as response:
                    if response.status == 200:
                        return True
                    else:
                        return False
        except Exception as e:
            print(
                f"exception happened in reset_clients_trafic : {str(e)} , server_id is {self.id}")
            return False
