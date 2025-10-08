import asyncio
from configparser import ConfigParser
import hashlib
import json
import random
import time
import signal
import sys
from threading import Thread
from servers_helper_queue import ServersHelperQueue
from shared_functions import execute_query_to_main_database, execute_query_to_queue_database
import psycopg2
from psycopg2.pool import SimpleConnectionPool
from psycopg2 import extras
from asyncio import Queue, Lock
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
import asyncio
import time
from collections import defaultdict, deque
from flask import Flask, jsonify, request
from flask_cors import CORS
import redis

class MillisecondRateLimiter:
    def __init__(self, rate, per_milliseconds):
        """
        Initialize rate limiter with millisecond precision
        
        Args:
            rate: Number of requests allowed
            per_milliseconds: Time window in milliseconds
        """
        self.rate = rate
        self.per_milliseconds = per_milliseconds
        self.allowance = float(rate)
        self.last_check = time.perf_counter()
        self.lock = asyncio.Lock()
        self.failure_count = 0
        self.last_failure_time = 0

    async def try_acquire(self):
        """Try to acquire without waiting. Returns True if allowed, False if limited."""
        async with self.lock:
            now = time.perf_counter()
            time_passed_ms = (now - self.last_check) * 1000
            self.last_check = now
            
            if self.failure_count > 0 and (now - self.last_failure_time) * 1000 < 30000:
                backoff_time_ms = min(2 ** self.failure_count * 1000, 60000)
                if (now - self.last_failure_time) * 1000 < backoff_time_ms:
                    return False
            
            self.allowance += time_passed_ms * (self.rate / self.per_milliseconds)
            if self.allowance > self.rate:
                self.allowance = self.rate
                
            if self.allowance >= 1.0:
                self.allowance -= 1.0
                return True
            return False

    def record_failure(self):
        """Record a failure to implement exponential backoff"""
        self.failure_count = min(self.failure_count + 1, 5)
        self.last_failure_time = time.perf_counter()

    def record_success(self):
        """Record a success to reset failure count"""
        self.failure_count = max(0, self.failure_count - 1)

    def get_wait_time_ms(self):
        """Get the time to wait in milliseconds before next request can be made"""
        if self.allowance >= 1.0:
            return 0
        return int((1.0 - self.allowance) * (self.per_milliseconds / self.rate))


server_mode = 'production'
config = ConfigParser()
config.read('config.ini')
postgress_user_name = config['default']['postgress_user_name']
postgress_user_password = config['default']['postgress_user_password']
postgress_db_name = config['default']['postgress_db_name']
postgress_ip = config['default']['postgress_ip']
to_iran_proxy_list_for_workers_pool_str = config.get('default', 'to_iran_proxy_list_for_workers_pool')
to_iran_proxy_list_for_workers_pool = json.loads(to_iran_proxy_list_for_workers_pool_str)
out_iran_proxy_list_for_workers_pool_str = config.get('default', 'out_iran_proxy_list_for_workers_pool')
out_iran_proxy_list_for_workers_pool = json.loads(out_iran_proxy_list_for_workers_pool_str)

kafka_bootstrap_servers = 'localhost:9092'
kafka_group_id = 'task-pool-group'
kafka_topic = 'client-management-queue'
kafka_deletion_topic = 'task-deletion-queue'


class RoundRobinTaskPool:
    def __init__(self):
        self.server_queues = defaultdict(lambda: asyncio.Queue())
        self.server_order = deque()
        self.active_servers = set()
        self.round_robin_lock = asyncio.Lock()
        
        self.workers = []
        self.running = False
        self.shutting_down = False
        self.loop = asyncio.new_event_loop()
        self.thread = None
        self.num_workers = 32
        
        self.out_iran_proxies = out_iran_proxy_list_for_workers_pool
        self.to_iran_proxies = to_iran_proxy_list_for_workers_pool
        self.current_out_iran_proxy = 0
        self.current_to_iran_proxy = 0
        
        self.psg_pool = SimpleConnectionPool(
            minconn=1,
            maxconn=3,
            user=postgress_user_name,
            password=postgress_user_password,
            host=postgress_ip,
            port="5432",
            database=postgress_db_name,
            cursor_factory=extras.DictCursor
        )
        
        self.server_limiters = defaultdict(lambda: MillisecondRateLimiter(1, 3000))
        
        self.task_counter = 0
        self.task_lookup = {}  # task_id -> (server_id, task_data)
        self.pending_tasks_by_server = defaultdict(deque)
        self.task_management_lock = asyncio.Lock()
        
        self.in_flight_tasks = set()
        self.requeue_lock = asyncio.Lock()
        
        self.kafka_consumer_config = {
            'bootstrap.servers': kafka_bootstrap_servers,
            'group.id': kafka_group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
        }
        self.deletion_consumer_config = {
            'bootstrap.servers': kafka_bootstrap_servers,
            'group.id': 'task-deletion-group',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
        }
        self.kafka_producer_config = {
            'bootstrap.servers': kafka_bootstrap_servers,
            'acks': 'all',
            'retries': 3,
            'retry.backoff.ms': 100,
        }
        
        self.kafka_consumer = None
        self.deletion_consumer = None
        self.kafka_producer = None
        
        self.redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

    def get_next_proxy(self, send_to_iran=False):
        """Get next proxy in round-robin fashion"""
        if not self.out_iran_proxies and not send_to_iran:
            raise ValueError("Out-Iran proxy list is empty")
        if not self.to_iran_proxies and send_to_iran:
            raise ValueError("To-Iran proxy list is empty")
        
        if send_to_iran:
            proxy = self.to_iran_proxies[self.current_to_iran_proxy]
            self.current_to_iran_proxy = (self.current_to_iran_proxy + 1) % len(self.to_iran_proxies)
        else:
            proxy = self.out_iran_proxies[self.current_out_iran_proxy]
            self.current_out_iran_proxy = (self.current_out_iran_proxy + 1) % len(self.out_iran_proxies)
        return proxy

    async def add_task_with_id(self, task):
        """Add a task with unique ID using round-robin distribution"""
        async with self.task_management_lock:
            self.task_counter += 1
            task_id = f"task_{self.task_counter}_{int(time.time() * 1000)}"
            
            operation, queue_table_id, server_id, email, uuid, inbound_id, client_flow, stringLowerCaseBooleanExpressinForEnable, limitIp, totalGB, expiryTime, vpn_protocol = task
            
            task_with_id = (task_id, *task)
            self.pending_tasks_by_server[server_id].append(task_with_id)
            self.task_lookup[task_id] = (server_id, task_with_id)
            
            await self.server_queues[server_id].put(task_with_id)
            
            async with self.round_robin_lock:
                if server_id not in self.active_servers:
                    self.active_servers.add(server_id)
                    self.server_order.append(server_id)
            
            return task_id

    async def get_next_task_round_robin(self):
        """Get next task using round-robin across all active servers"""
        async with self.round_robin_lock:
            if not self.server_order:
                return None
            
            attempts = 0
            max_attempts = len(self.server_order)
            
            while attempts < max_attempts:
                if not self.server_order:
                    return None
                    
                current_server = self.server_order[0]
                self.server_order.rotate(-1)  # Move to next server for next call
                
                server_queue = self.server_queues.get(current_server)
                if server_queue is not None:
                    try:
                        task = server_queue.get_nowait()
                        return task
                    except asyncio.QueueEmpty:
                        attempts += 1
                        continue
                else:
                    attempts += 1
                    continue
                
            return None

    async def cleanup_empty_servers(self):
        """Remove servers with no pending tasks from round-robin rotation"""
        async with self.round_robin_lock:
            servers_to_remove = []
            
            for server_id in list(self.active_servers):
                queue = self.server_queues.get(server_id)
                pending_tasks = self.pending_tasks_by_server.get(server_id, deque())
                
                if (queue is None or queue.empty()) and len(pending_tasks) == 0:
                    servers_to_remove.append(server_id)
            
            for server_id in servers_to_remove:
                self.active_servers.discard(server_id)
                temp_order = []
                while self.server_order:
                    srv = self.server_order.popleft()
                    if srv != server_id:
                        temp_order.append(srv)
                self.server_order.extend(temp_order)

    async def worker(self, worker_id):
        """Worker process with round-robin task distribution"""
        while self.running and not self.shutting_down:
            try:
                task = await self.get_next_task_round_robin()
                
                if task is None:
                    await self.cleanup_empty_servers()
                    await asyncio.sleep(0.1)
                    continue
                
                if self.shutting_down:
                    await self.requeue_task_to_kafka(task)
                    continue
                
                task_id = task[0]
                operation, queue_table_id, server_id, email, uuid, inbound_id, client_flow, stringLowerCaseBooleanExpressinForEnable, limitIp, totalGB, expiryTime, vpn_protocol = task[1:]
                
                limiter = self.server_limiters[server_id]
                if not await limiter.try_acquire():
                    if self.shutting_down:
                        await self.requeue_task_to_kafka(task)
                    else:
                        await self.server_queues[server_id].put(task)
                        await asyncio.sleep(0.001)
                    continue
                
                if operation == "change_state":
                    task_counts = await self.count_pending_tasks_by_server_id(server_id)
                    total_tasks = task_counts['total']
                    
                    if total_tasks > 10:
                        print(f"Server {server_id} has {total_tasks} tasks. Putting change_state back in queue.")
                        await self.server_queues[server_id].put(task)
                        await asyncio.sleep(0.1)
                        continue
                
                async with self.task_management_lock:
                    if task_id in self.task_lookup:
                        del self.task_lookup[task_id]
                    if server_id in self.pending_tasks_by_server:
                        try:
                            for i, pending_task in enumerate(self.pending_tasks_by_server[server_id]):
                                if pending_task[0] == task_id:
                                    del self.pending_tasks_by_server[server_id][i]
                                    break
                        except (ValueError, IndexError):
                            pass
                
                flight_task_id = f"{worker_id}_{time.time()}"
                async with self.requeue_lock:
                    self.in_flight_tasks.add((flight_task_id, task))
                
                print(f"{time.strftime('%Y-%m-%d %H:%M:%S')}.{int((time.time() % 1) * 1000):03d} - Processing task for server {server_id}")
                success = False

                try:
                    if operation == "add":
                        proxy = self.get_next_proxy()
                        server_helper = ServersHelperQueue(server_id, proxy)
                        success = await server_helper.queue_add_client_even_if_exist(email, uuid, inbound_id, client_flow, stringLowerCaseBooleanExpressinForEnable, limitIp, totalGB, expiryTime, vpn_protocol)
                    elif operation == "add_if_not_exist":
                        proxy = self.get_next_proxy()
                        server_helper = ServersHelperQueue(server_id, proxy)
                        success = await server_helper.queue_add_client_if_not_exist(email, uuid, inbound_id, client_flow, stringLowerCaseBooleanExpressinForEnable, limitIp, totalGB, expiryTime, vpn_protocol)
                    elif operation == "delete":
                        proxy = self.get_next_proxy()
                        server_helper = ServersHelperQueue(server_id, proxy)
                        success = await server_helper.queue_dl_client(inbound_id, uuid, vpn_protocol, email)
                    elif operation == "add_in_iran":
                        proxy = self.get_next_proxy(send_to_iran=True)
                        server_helper = ServersHelperQueue(server_id, proxy)
                        success = await server_helper.queue_add_client_even_if_exist(email, uuid, inbound_id, client_flow, stringLowerCaseBooleanExpressinForEnable, limitIp, totalGB, expiryTime, vpn_protocol)
                    elif operation == "add_if_not_exist_in_iran":
                        proxy = self.get_next_proxy(send_to_iran=True)
                        server_helper = ServersHelperQueue(server_id, proxy)
                        success = await server_helper.queue_add_client_if_not_exist(email, uuid, inbound_id, client_flow, stringLowerCaseBooleanExpressinForEnable, limitIp, totalGB, expiryTime, vpn_protocol)
                    elif operation == "delete_in_iran":
                        proxy = self.get_next_proxy(send_to_iran=True)
                        server_helper = ServersHelperQueue(server_id, proxy)
                        success = await server_helper.queue_dl_client(inbound_id, uuid, vpn_protocol, email)
                    elif operation == "change_state":
                        self.change_server_state(server_id)
                        success = True
                        
                    if success:
                        limiter.record_success()
                    else:
                        limiter.record_failure()
                        print(f"Task failed for {email} on server {server_id}")

                except Exception as e:
                    limiter.record_failure()
                    print(f"Task execution error: {str(e)}")
                    success = False
                
                async with self.requeue_lock:
                    self.in_flight_tasks.discard((flight_task_id, task))
                
                task_counts = await self.count_pending_tasks_by_server_id(server_id)
                try:
                    redis_key = f"server:{server_id}:task_count"
                    redis_data = {
                        'pending': task_counts['pending'],
                        'in_flight': task_counts['in_flight'],
                        'total': task_counts['total'],
                        'last_updated': int(time.time() * 1000)
                    }
                    self.redis_client.hset(redis_key, mapping=redis_data)
                    self.redis_client.expire(redis_key, 30)
                except Exception as redis_error:
                    print(f"Failed to write to Redis for server {server_id}: {redis_error}")
                
                print(f"Worker {worker_id} completed task: {operation} {server_id} {email} - Success: {success}")
                
            except asyncio.CancelledError:
                print(f"Worker {worker_id} cancelled gracefully")
                break
            except Exception as e:
                print(f"Worker {worker_id} encountered an error: {str(e)}")

    async def count_pending_tasks_by_server_id(self, server_id):
        """Count pending tasks for a specific server_id"""
        async with self.task_management_lock:
            pending_count = len(self.pending_tasks_by_server.get(server_id, []))
            
            in_flight_count = 0
            async with self.requeue_lock:
                for task_id_flight, task_data in self.in_flight_tasks:
                    if task_data[3] == server_id:
                        in_flight_count += 1
            
            return {
                'pending': pending_count,
                'in_flight': in_flight_count,
                'total': pending_count + in_flight_count
            }

    async def remove_tasks_by_server_id(self, server_id):
        """Remove all pending tasks for a specific server_id"""
        async with self.task_management_lock:
            removed_count = 0
            
            if server_id in self.pending_tasks_by_server:
                tasks_to_remove = list(self.pending_tasks_by_server[server_id])
                
                self.pending_tasks_by_server[server_id].clear()
                
                for task_with_id in tasks_to_remove:
                    task_id = task_with_id[0]
                    if task_id in self.task_lookup:
                        del self.task_lookup[task_id]
                        removed_count += 1
                
                if server_id in self.server_queues:
                    self.server_queues[server_id] = asyncio.Queue()
                
                async with self.round_robin_lock:
                    if server_id in self.active_servers:
                        self.active_servers.discard(server_id)
                        temp_order = []
                        while self.server_order:
                            srv = self.server_order.popleft()
                            if srv != server_id:
                                temp_order.append(srv)
                        self.server_order.extend(temp_order)
                
                in_flight_to_remove = []
                async with self.requeue_lock:
                    for task_id_flight, task_data in list(self.in_flight_tasks):
                        if task_data[3] == server_id:
                            in_flight_to_remove.append((task_id_flight, task_data))
                    
                    for item in in_flight_to_remove:
                        self.in_flight_tasks.discard(item)
                        removed_count += 1
            
            return removed_count

    async def requeue_task_to_kafka(self, task):
        """Requeue a task back to Kafka"""
        if not self.kafka_producer:
            return False
            
        try:
            task_id, operation, queue_table_id, server_id, email, uuid, inbound_id, client_flow, stringLowerCaseBooleanExpressinForEnable, limitIp, totalGB, expiryTime, vpn_protocol = task
            
            task_data = {
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
                'requeued_at': time.time(),
                'requeue_reason': 'graceful_shutdown'
            }
            
            def delivery_callback(err, msg):
                if err:
                    print(f"Failed to requeue task: {err}")
                else:
                    print(f"Successfully requeued task for email {email} to partition {msg.partition()}")
            
            self.kafka_producer.produce(
                kafka_topic,
                key=f"{server_id}_{email}",
                value=json.dumps(task_data),
                callback=delivery_callback
            )
            
            return True
            
        except Exception as e:
            print(f"Error requeueing task to Kafka: {str(e)}")
            return False

    async def requeue_all_pending_tasks(self):
        """Requeue all pending tasks back to Kafka during shutdown"""
        print("Requeueing all pending tasks back to Kafka...")
        requeued_count = 0
        
        if not self.kafka_producer:
            self.kafka_producer = Producer(self.kafka_producer_config)
        
        async with self.round_robin_lock:
            for server_id in list(self.active_servers):
                server_queue = self.server_queues.get(server_id)
                if server_queue:
                    while not server_queue.empty():
                        try:
                            task = server_queue.get_nowait()
                            if await self.requeue_task_to_kafka(task):
                                requeued_count += 1
                        except asyncio.QueueEmpty:
                            break
        
        async with self.requeue_lock:
            for task_id, task in list(self.in_flight_tasks):
                if await self.requeue_task_to_kafka(task):
                    requeued_count += 1
        
        if self.kafka_producer:
            self.kafka_producer.flush(timeout=10)
        
        print(f"Successfully requeued {requeued_count} tasks back to Kafka")

    def change_server_state(self, server_id):
        """Change server state in database"""
        try:
            conn = self.psg_pool.getconn()
            cursor = conn.cursor()
            cursor = execute_query_to_main_database(cursor,
                                                    'UPDATE servers SET _state = _state + 1 WHERE _id = %s', (server_id,))
            conn.commit()
        
            import sqlite3
            sqlite_conn = sqlite3.connect('client-management-queue.db')
            sqlite_cursor = sqlite_conn.cursor()
            
            execute_query_to_queue_database(sqlite_cursor, 'UPDATE servers SET _last_time_calculated = ? where server_id = ?', ((time.time() - 1780), server_id,))
            sqlite_conn.commit()
            sqlite_conn.close()
        
        except Exception as e:
            print(f"Error changing server state: {str(e)}")
        finally:
            self.psg_pool.putconn(conn)

    async def get_all_server_task_counts(self):
        """Get task counts for all servers"""
        async with self.task_management_lock:
            server_counts = {}
            
            for server_id, tasks in self.pending_tasks_by_server.items():
                server_counts[server_id] = {
                    'pending': len(tasks),
                    'in_flight': 0,
                    'total': len(tasks)
                }
            
            async with self.requeue_lock:
                for task_id_flight, task_data in self.in_flight_tasks:
                    server_id = task_data[3]
                    if server_id not in server_counts:
                        server_counts[server_id] = {'pending': 0, 'in_flight': 0, 'total': 0}
                    server_counts[server_id]['in_flight'] += 1
                    server_counts[server_id]['total'] += 1
            
            return server_counts

    def start(self):
        """Start the task pool"""
        if not self.running:
            self.running = True
            self.thread = Thread(target=self._run_event_loop)
            self.thread.start()
            print(f"Round-Robin Task pool started with {self.num_workers} workers")

    def _run_event_loop(self):
        """Run the asyncio event loop"""
        asyncio.set_event_loop(self.loop)
        self.workers = [self.loop.create_task(self.worker(i)) for i in range(self.num_workers)]
        self.loop.create_task(self.continuous_task_addition())
        self.loop.create_task(self.continuous_task_deletion_consumer())
        self.loop.run_forever()

    def stop(self):
        """Graceful shutdown with task requeueing"""
        if not self.running:
            return
            
        print("Initiating graceful shutdown...")
        self.shutting_down = True
        
        if self.kafka_consumer:
            print("Stopping Kafka consumer...")
            self.kafka_consumer.close()
            
        if self.deletion_consumer:
            print("Stopping deletion consumer...")
            self.deletion_consumer.close()
        
        def shutdown_sequence():
            try:
                print("Waiting for workers to finish current tasks...")
                start_time = time.time()
                while time.time() - start_time < 10:
                    total_pending = sum(len(queue._queue) for queue in self.server_queues.values())
                    if total_pending == 0 and len(self.in_flight_tasks) == 0:
                        break
                    time.sleep(0.1)
                
                asyncio.run_coroutine_threadsafe(
                    self.requeue_all_pending_tasks(), self.loop
                ).result(timeout=15)
                
            except Exception as e:
                print(f"Error during shutdown sequence: {str(e)}")
            finally:
                self.running = False
                
                for worker in self.workers:
                    worker.cancel()
                
                if self.kafka_producer:
                    try:
                        self.kafka_producer.flush(timeout=5)
                    except Exception as e:
                        print(f"Error flushing Kafka producer: {str(e)}")
                    finally:
                        self.kafka_producer = None
                
                time.sleep(0.5)
                self.loop.call_soon_threadsafe(self.loop.stop)
                
        shutdown_thread = Thread(target=shutdown_sequence)
        shutdown_thread.start()
        shutdown_thread.join(timeout=20)
        
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=5)
        
        try:
            self.psg_pool.closeall()
        except Exception as e:
            print(f"Error closing database pool: {str(e)}")
        
        self.workers.clear()
        print("Round-Robin Task pool stopped gracefully")

    async def continuous_task_deletion_consumer(self):
        """Kafka consumer for task deletion commands"""
        self.deletion_consumer = Consumer(self.deletion_consumer_config)
        self.deletion_consumer.subscribe([kafka_deletion_topic])
        
        print(f"Kafka deletion consumer started for topic: {kafka_deletion_topic}")
        
        while self.running and not self.shutting_down:
            try:
                msg = self.deletion_consumer.poll(1.0)
                
                if msg is None:
                    await asyncio.sleep(0.1)
                    continue
                    
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        print(f"Reached end of deletion partition {msg.partition()}")
                    elif msg.error():
                        raise KafkaException(msg.error())
                else:
                    if self.shutting_down:
                        break
                        
                    try:
                        deletion_data = json.loads(msg.value().decode('utf-8'))
                        print(f"Received deletion command: {deletion_data}")
                        
                        command = deletion_data.get('command')
                        server_id = deletion_data.get('server_id')
                        
                        if command == 'delete_tasks_by_server' and server_id:
                            removed_count = await self.remove_tasks_by_server_id(server_id)
                            print(f"Removed {removed_count} tasks for server {server_id}")
                        
                        self.deletion_consumer.commit(msg)
                        
                    except json.JSONDecodeError as e:
                        print(f"Failed to parse deletion message: {e}")
                        self.deletion_consumer.commit(msg)
                    except Exception as e:
                        print(f"Error processing deletion message: {e}")
                        
            except Exception as e:
                if not self.shutting_down:
                    print(f"Error in deletion consumer: {str(e)}")
                    await asyncio.sleep(1)
    
    async def continuous_task_addition(self):
        """Kafka consumer with batch processing"""
        self.kafka_consumer = Consumer(self.kafka_consumer_config)
        self.kafka_producer = Producer(self.kafka_producer_config)
        self.kafka_consumer.subscribe([kafka_topic])
        
        print(f"Kafka consumer started for topic: {kafka_topic}")
        
        batch_size = 10
        batch_timeout = 0.1  # 100ms
        message_batch = []
        last_batch_time = time.time()
        
        while self.running and not self.shutting_down:
            try:
                msg = self.kafka_consumer.poll(0.1)  # Shorter poll timeout
                
                if msg is None:
                    if message_batch and (time.time() - last_batch_time) > batch_timeout:
                        await self.process_message_batch(message_batch)
                        message_batch = []
                        last_batch_time = time.time()
                    await asyncio.sleep(0.01)
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        print(f"Reached end of partition {msg.partition()}")
                    elif msg.error():
                        raise KafkaException(msg.error())
                else:
                    if self.shutting_down:
                        break
                    
                    message_batch.append(msg)
                    
                    if (len(message_batch) >= batch_size or 
                        (time.time() - last_batch_time) > batch_timeout):
                        await self.process_message_batch(message_batch)
                        message_batch = []
                        last_batch_time = time.time()
                            
            except Exception as e:
                if not self.shutting_down:
                    print(f"Error in Kafka consumer: {str(e)}")
                    await asyncio.sleep(1)
        
        if message_batch:
            await self.process_message_batch(message_batch)

    async def process_message_batch(self, messages):
        """Process a batch of messages with minimal lock time"""
        if not messages:
            return
        
        tasks_to_add = []
        messages_to_commit = []
        
        for msg in messages:
            try:
                task_data = json.loads(msg.value().decode('utf-8'))
                
                operation = task_data.get('operation')
                server_id = task_data.get('server_id')
                email = task_data.get('email')
                uuid = task_data.get('uuid')
                inbound_id = task_data.get('inbound_id')
                client_flow = task_data.get('client_flow')
                stringLowerCaseBooleanExpressinForEnable = task_data.get('stringLowerCaseBooleanExpressinForEnable')
                limitIp = task_data.get('limitIp')
                totalGB = task_data.get('totalGB')
                expiryTime = task_data.get('expiryTime')
                vpn_protocol = task_data.get('vpn_protocol')
                
                queue_table_id = f"kafka_{msg.topic()}_{msg.partition()}_{msg.offset()}"
                
                task = (operation, queue_table_id, server_id, email, uuid, inbound_id,
                    client_flow, stringLowerCaseBooleanExpressinForEnable, limitIp, totalGB, expiryTime, vpn_protocol)
                
                tasks_to_add.append(task)
                messages_to_commit.append(msg)
                
            except json.JSONDecodeError as e:
                print(f"Failed to parse Kafka message: {e}")
                messages_to_commit.append(msg)  # Still commit bad messages
            except Exception as e:
                print(f"Error processing Kafka message: {e}")
        
        if tasks_to_add:
            await self.add_tasks_batch(tasks_to_add)
        
        for msg in messages_to_commit:
            self.kafka_consumer.commit(msg)

    async def add_tasks_batch(self, tasks):
        """Add multiple tasks with a single lock acquisition"""
        async with self.task_management_lock:
            servers_updated = set()
            
            for task in tasks:
                self.task_counter += 1
                task_id = f"task_{self.task_counter}_{int(time.time() * 1000)}"
                
                operation, queue_table_id, server_id, email, uuid, inbound_id, client_flow, stringLowerCaseBooleanExpressinForEnable, limitIp, totalGB, expiryTime, vpn_protocol = task
                
                task_with_id = (task_id, *task)
                self.pending_tasks_by_server[server_id].append(task_with_id)
                self.task_lookup[task_id] = (server_id, task_with_id)
                
                await self.server_queues[server_id].put(task_with_id)
                servers_updated.add(server_id)
            
            async with self.round_robin_lock:
                for server_id in servers_updated:
                    if server_id not in self.active_servers:
                        self.active_servers.add(server_id)
                        self.server_order.append(server_id)

    def remove_tasks_by_server_sync(self, server_id):
        """Synchronous wrapper for removing tasks by server ID"""
        if self.loop and self.loop.is_running():
            future = asyncio.run_coroutine_threadsafe(
                self.remove_tasks_by_server_id(server_id), self.loop
            )
            return future.result(timeout=10)
        return 0

    def count_tasks_by_server_sync(self, server_id):
        """Synchronous wrapper for counting tasks by server ID"""
        if self.loop and self.loop.is_running():
            future = asyncio.run_coroutine_threadsafe(
                self.count_pending_tasks_by_server_id(server_id), self.loop
            )
            return future.result(timeout=10)
        return {'pending': 0, 'in_flight': 0, 'total': 0}

    def get_all_server_counts_sync(self):
        """Synchronous wrapper for getting all server task counts"""
        if self.loop and self.loop.is_running():
            future = asyncio.run_coroutine_threadsafe(
                self.get_all_server_task_counts(), self.loop
            )
            return future.result(timeout=10)
        return {}


class TaskPoolManager:
    """External interface for managing the task pool"""
    
    def __init__(self, task_pool_instance):
        self.pool = task_pool_instance
    
    def remove_server_tasks(self, server_id):
        """Remove all tasks for a specific server"""
        try:
            removed_count = self.pool.remove_tasks_by_server_sync(server_id)
            return {
                'success': True,
                'removed_count': removed_count,
                'message': f"Successfully removed {removed_count} tasks for server {server_id}"
            }
        except Exception as e:
            return {
                'success': False,
                'removed_count': 0,
                'message': f"Error removing tasks: {str(e)}"
            }
    
    def get_server_task_count(self, server_id):
        """Get task count for a specific server"""
        try:
            counts = self.pool.count_tasks_by_server_sync(server_id)
            return {
                'success': True,
                'server_id': server_id,
                'counts': counts
            }
        except Exception as e:
            return {
                'success': False,
                'server_id': server_id,
                'counts': {'pending': 0, 'in_flight': 0, 'total': 0},
                'message': f"Error getting task count: {str(e)}"
            }
    
    def get_all_server_counts(self):
        """Get task counts for all servers"""
        try:
            counts = self.pool.get_all_server_counts_sync()
            return {
                'success': True,
                'server_counts': counts
            }
        except Exception as e:
            return {
                'success': False,
                'server_counts': {},
                'message': f"Error getting server counts: {str(e)}"
            }


class TaskPoolAPI:
    """REST API interface for task pool management"""
    
    def __init__(self, task_manager, host='0.0.0.0', port=5000):
        self.task_manager = task_manager
        self.host = host
        self.port = port
        self.app = Flask(__name__)
        CORS(self.app)
        
        self.app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True
        self._register_routes()
        
        self.api_thread = None
        self.running = False

    def _register_routes(self):
        """Register all API routes"""
        
        @self.app.route('/health', methods=['GET'])
        def health_check():
            """Health check endpoint"""
            return jsonify({
                'status': 'healthy',
                'timestamp': time.time(),
                'service': 'round-robin-task-pool-api'
            })

        @self.app.route('/api/v1/tasks/count/<int:server_id>', methods=['GET'])
        def count_tasks_by_server(server_id):
            """Count tasks for a specific server"""
            try:
                result = self.task_manager.get_server_task_count(server_id)
                
                if result['success']:
                    return jsonify({
                        'success': True,
                        'server_id': server_id,
                        'data': result['counts'],
                        'timestamp': time.time()
                    })
                else:
                    return jsonify({
                        'success': False,
                        'server_id': server_id,
                        'error': result.get('message', 'Unknown error'),
                        'timestamp': time.time()
                    }), 500
                    
            except Exception as e:
                return jsonify({
                    'success': False,
                    'server_id': server_id,
                    'error': str(e),
                    'timestamp': time.time()
                }), 500

        @self.app.route('/api/v1/tasks/count', methods=['GET'])
        def count_all_server_tasks():
            """Count tasks for all servers"""
            try:
                result = self.task_manager.get_all_server_counts()
                
                if result['success']:
                    total_pending = sum(counts['pending'] for counts in result['server_counts'].values())
                    total_in_flight = sum(counts['in_flight'] for counts in result['server_counts'].values())
                    total_tasks = sum(counts['total'] for counts in result['server_counts'].values())
                    
                    return jsonify({
                        'success': True,
                        'data': {
                            'servers': result['server_counts'],
                            'summary': {
                                'total_pending': total_pending,
                                'total_in_flight': total_in_flight,
                                'total_tasks': total_tasks,
                                'server_count': len(result['server_counts'])
                            }
                        },
                        'timestamp': time.time()
                    })
                else:
                    return jsonify({
                        'success': False,
                        'error': result.get('message', 'Unknown error'),
                        'timestamp': time.time()
                    }), 500
                    
            except Exception as e:
                return jsonify({
                    'success': False,
                    'error': str(e),
                    'timestamp': time.time()
                }), 500

        # @self.app.route('/api/v1/tasks/remove/<int:server_id>', methods=['DELETE'])
        # def remove_tasks_by_server(server_id):
        #     """Remove all tasks for a specific server"""
        #     try:
        #         result = self.task_manager.remove_server_tasks(server_id)
                
        #         if result['success']:
        #             return jsonify({
        #                 'success': True,
        #                 'server_id': server_id,
        #                 'removed_count': result['removed_count'],
        #                 'message': result['message'],
        #                 'timestamp': time.time()
        #             })
        #         else:
        #             return jsonify({
        #                 'success': False,
        #                 'server_id': server_id,
        #                 'error': result.get('message', 'Unknown error'),
        #                 'timestamp': time.time()
        #             }), 500
                    
        #     except Exception as e:
        #         return jsonify({
        #             'success': False,
        #             'server_id': server_id,
        #             'error': str(e),
        #             'timestamp': time.time()
        #         }), 500


        # @self.app.route('/api/v1/tasks/remove', methods=['POST'])
        # def remove_tasks_by_multiple_servers():
        #     """Remove tasks for multiple servers"""
        #     try:
        #         data = request.get_json()
        #         if not data or 'server_ids' not in data:
        #             return jsonify({
        #                 'success': False,
        #                 'error': 'Missing server_ids in request body',
        #                 'timestamp': time.time()
        #             }), 400
                
        #         server_ids = data['server_ids']
        #         if not isinstance(server_ids, list):
        #             return jsonify({
        #                 'success': False,
        #                 'error': 'server_ids must be a list',
        #                 'timestamp': time.time()
        #             }), 400
                
        #         results = {}
        #         total_removed = 0
                
        #         for server_id in server_ids:
        #             try:
        #                 result = self.task_manager.remove_server_tasks(server_id)
        #                 results[str(server_id)] = result
        #                 if result['success']:
        #                     total_removed += result['removed_count']
        #             except Exception as e:
        #                 results[str(server_id)] = {
        #                     'success': False,
        #                     'error': str(e),
        #                     'removed_count': 0
        #                 }
                
        #         return jsonify({
        #             'success': True,
        #             'total_removed': total_removed,
        #             'results': results,
        #             'timestamp': time.time()
        #         })
                
        #     except Exception as e:
        #         return jsonify({
        #             'success': False,
        #             'error': str(e),
        #             'timestamp': time.time()
        #         }), 500


        @self.app.route('/api/v1/tasks/stats', methods=['GET'])
        def get_detailed_stats():
            """Get detailed statistics about the task pool"""
            try:
                count_result = self.task_manager.get_all_server_counts()
                
                if not count_result['success']:
                    return jsonify({
                        'success': False,
                        'error': count_result.get('message', 'Failed to get server counts'),
                        'timestamp': time.time()
                    }), 500
                
                server_counts = count_result['server_counts']
                
                stats = {
                    'servers': {
                        'total': len(server_counts),
                        'with_pending_tasks': len([s for s in server_counts.values() if s['pending'] > 0]),
                        'with_in_flight_tasks': len([s for s in server_counts.values() if s['in_flight'] > 0])
                    },
                    'tasks': {
                        'total_pending': sum(s['pending'] for s in server_counts.values()),
                        'total_in_flight': sum(s['in_flight'] for s in server_counts.values()),
                        'total_tasks': sum(s['total'] for s in server_counts.values())
                    },
                    'top_servers': {}
                }
                
                if server_counts:
                    sorted_servers = sorted(
                        server_counts.items(), 
                        key=lambda x: x[1]['total'], 
                        reverse=True
                    )[:10]
                    
                    stats['top_servers'] = {
                        str(server_id): counts for server_id, counts in sorted_servers
                    }
                
                return jsonify({
                    'success': True,
                    'data': stats,
                    'timestamp': time.time()
                })
                
            except Exception as e:
                return jsonify({
                    'success': False,
                    'error': str(e),
                    'timestamp': time.time()
                }), 500

        # @self.app.route('/api/v1/docs', methods=['GET'])
        # def api_documentation():
        #     """API documentation endpoint"""
        #     docs = {
        #         'service': 'Round-Robin Task Pool Management API',
        #         'version': 'v1',
        #         'description': 'Fair task distribution across servers using round-robin algorithm',
        #         'endpoints': {
        #             'GET /health': {
        #                 'description': 'Health check endpoint',
        #                 'response': 'Service status information'
        #             },
        #             'GET /api/v1/tasks/count/<server_id>': {
        #                 'description': 'Count tasks for a specific server',
        #                 'parameters': {'server_id': 'Integer - Server ID'},
        #                 'response': 'Task counts (pending, in_flight, total)'
        #             },
        #             'GET /api/v1/tasks/count': {
        #                 'description': 'Count tasks for all servers',
        #                 'response': 'Task counts for all servers with summary'
        #             },
        #             'DELETE /api/v1/tasks/remove/<server_id>': {
        #                 'description': 'Remove all tasks for a specific server',
        #                 'parameters': {'server_id': 'Integer - Server ID'},
        #                 'response': 'Removal result and count'
        #             },
        #             'POST /api/v1/tasks/remove': {
        #                 'description': 'Remove tasks for multiple servers',
        #                 'body': {'server_ids': 'List of server IDs'},
        #                 'response': 'Removal results for each server'
        #             },
        #             'GET /api/v1/tasks/stats': {
        #                 'description': 'Get detailed task pool statistics',
        #                 'response': 'Comprehensive statistics and top servers'
        #             },
        #             'GET /api/v1/docs': {
        #                 'description': 'This documentation',
        #                 'response': 'API documentation'
        #             }
        #         }
        #     }
            
        #     return jsonify(docs)

        @self.app.errorhandler(404)
        def not_found(error):
            return jsonify({
                'success': False,
                'error': 'Endpoint not found',
                'message': 'Use GET /api/v1/docs for available endpoints',
                'timestamp': time.time()
            }), 404

        @self.app.errorhandler(500)
        def internal_error(error):
            return jsonify({
                'success': False,
                'error': 'Internal server error',
                'timestamp': time.time()
            }), 500

    def start(self):
        """Start the API server in a separate thread"""
        if not self.running:
            self.running = True
            self.api_thread = Thread(target=self._run_flask_app, daemon=True)
            self.api_thread.start()
            print(f"Round-Robin Task Pool API started on http://{self.host}:{self.port}")
            print(f"API Documentation: http://{self.host}:{self.port}/api/v1/docs")

    def _run_flask_app(self):
        """Run Flask app with error handling"""
        try:
            import logging
            log = logging.getLogger('werkzeug')
            log.setLevel(logging.ERROR)
            
            self.app.run(
                host=self.host,
                port=self.port,
                debug=False,
                threaded=True,
                use_reloader=False
            )
        except Exception as e:
            print(f"Error running Flask app: {str(e)}")

    def stop(self):
        """Stop the API server"""
        self.running = False
        print("Round-Robin Task Pool API stopped")


def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    print(f"Received signal {signum}, initiating graceful shutdown...")
    if 'pool' in globals():
        pool.stop()
    if 'api_server' in globals():
        api_server.stop()
    sys.exit(0)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    pool = RoundRobinTaskPool()
    pool.start()
    
    task_manager = TaskPoolManager(pool)
    
    api_server = TaskPoolAPI(task_manager, host='0.0.0.0', port=15000)
    api_server.start()
    
    print("Round-Robin Task Pool started with fair task distribution")
    print("Task Distribution: Each server gets equal processing opportunities")
    print("Example: Server 721 → Server 722 → Server 723 → Server 724 → Server 725 → repeat")
    print("")
    print("Available operations:")
    print("  - task_manager.remove_server_tasks(server_id)")
    print("  - task_manager.get_server_task_count(server_id)")
    print("  - task_manager.get_all_server_counts()")
    print("")
    print("API Endpoints:")
    print("  - GET  /health")
    print("  - GET  /api/v1/tasks/count/<server_id>")
    print("  - GET  /api/v1/tasks/count")
    print("  - DELETE /api/v1/tasks/remove/<server_id>")
    print("  - POST /api/v1/tasks/remove")
    print("  - GET  /api/v1/tasks/stats")
    print("  - GET  /api/v1/docs")
    print(f"  - API Documentation: http://localhost:15000/api/v1/docs")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down...")
        api_server.stop()
        pool.stop()
