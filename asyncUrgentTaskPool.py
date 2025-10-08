import asyncio
from configparser import ConfigParser
import hashlib
import json
import time
from threading import Thread
import sqlite3
from shared_functions import execute_query_to_queue_database
from servers_helper_queue import ServersHelperQueue
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
from collections import defaultdict

server_mode = 'production'
config = ConfigParser()
config.read('config.ini')
to_iran_proxy_list_for_workers_pool_str = config.get('default', 'to_iran_proxy_list_for_workers_pool')
to_iran_proxy_list_for_workers_pool = json.loads(to_iran_proxy_list_for_workers_pool_str)
out_iran_proxy_list_for_workers_pool_str = config.get('default', 'out_iran_proxy_list_for_workers_pool')
out_iran_proxy_list_for_workers_pool = json.loads(out_iran_proxy_list_for_workers_pool_str)

kafka_bootstrap_servers = 'localhost:9092'
kafka_group_id = 'urgent-task-pool-group'
kafka_topic = 'client-management-urgent-queue'


class AsyncUrgentTaskPool:
    def __init__(self):
        self.task_queue = asyncio.Queue()
        self.workers = []
        self.out_iran_proxies = out_iran_proxy_list_for_workers_pool
        self.to_iran_proxies = to_iran_proxy_list_for_workers_pool
        self.current_out_iran_proxy = 0
        self.current_to_iran_proxy = 0
        self.running = False
        self.shutting_down = False
        self.loop = asyncio.new_event_loop()
        self.thread = None
        self.num_workers = 6
        
        self.kafka_consumer_config = {
            'bootstrap.servers': kafka_bootstrap_servers,
            'group.id': kafka_group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
        }
        self.kafka_consumer = None
        
        self.kafka_producer_config = {
            'bootstrap.servers': kafka_bootstrap_servers,
            'acks': 'all',
            'retries': 3,
            'retry.backoff.ms': 100,
        }
        self.kafka_producer = None
        
        self.in_flight_tasks = set()
        self.requeue_lock = asyncio.Lock()

    def get_next_proxy(self, send_to_iran=False):
        if not self.out_iran_proxies and not send_to_iran:
            raise ValueError("Proxy list is empty")
        if not self.out_iran_proxies and send_to_iran:
            raise ValueError("Proxy list is empty")
        
        if send_to_iran:
            proxy = self.to_iran_proxies[self.current_to_iran_proxy]
            self.current_to_iran_proxy = (self.current_to_iran_proxy + 1) % len(self.to_iran_proxies)
        else:
            proxy = self.out_iran_proxies[self.current_out_iran_proxy]
            self.current_out_iran_proxy = (self.current_out_iran_proxy + 1) % len(self.out_iran_proxies)
        return proxy

    async def worker(self, worker_id):
        while self.running and not self.shutting_down:
            try:
                try:
                    task = await asyncio.wait_for(self.task_queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                
                if self.shutting_down:
                    await self.requeue_task_to_kafka(task)
                    self.task_queue.task_done()
                    continue
                
                operation, important_tasks_queue_table_id, server_id, email, uuid, inbound_id, client_flow, stringLowerCaseBooleanExpressinForEnable, limitIp, totalGB, expiryTime, vpn_protocol = task
                
                flight_task_id = f"{worker_id}_{time.time()}"
                async with self.requeue_lock:
                    self.in_flight_tasks.add((flight_task_id, task))
                
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

                if not success:
                    producer_config = {
                        'bootstrap.servers': kafka_bootstrap_servers,
                        'client.id': 'urgent-task-pool-retry'
                    }
                    
                    producer = Producer(producer_config)
                    
                    retry_task_data = {
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
                        'vpn_protocol': vpn_protocol
                    }
                    
                    producer.produce(
                        'client-management-queue',
                        key=str(server_id),
                        value=json.dumps(retry_task_data)
                    )
                    producer.flush()
                    print(f"Failed task sent to client-management-queue: {operation} for email {email}")
                
                async with self.requeue_lock:
                    self.in_flight_tasks.discard((flight_task_id, task))

                print(f"Worker {worker_id} completed task: {operation}")
                self.task_queue.task_done()
            except asyncio.CancelledError:
                print(f"Worker {worker_id} cancelled gracefully")
                break
            except Exception as e:
                print(f"Worker {worker_id} encountered an error: {str(e)}")

    async def requeue_task_to_kafka(self, task):
        """Requeue a task back to Kafka"""
        if not self.kafka_producer:
            self.kafka_producer = Producer(self.kafka_producer_config)
            
        try:
            operation, important_tasks_queue_table_id, server_id, email, uuid, inbound_id, client_flow, stringLowerCaseBooleanExpressinForEnable, limitIp, totalGB, expiryTime, vpn_protocol = task
            
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
                    print(f"Failed to requeue urgent task: {err}")
                else:
                    print(f"Successfully requeued urgent task for email {email} to partition {msg.partition()}")
            
            self.kafka_producer.produce(
                kafka_topic,
                key=f"{server_id}_{email}",
                value=json.dumps(task_data),
                callback=delivery_callback
            )
            
            return True
            
        except Exception as e:
            print(f"Error requeueing urgent task to Kafka: {str(e)}")
            return False

    async def requeue_all_pending_tasks(self):
        """Requeue all pending tasks back to Kafka during shutdown"""
        print("Requeueing all pending urgent tasks back to Kafka...")
        requeued_count = 0
        
        if not self.kafka_producer:
            self.kafka_producer = Producer(self.kafka_producer_config)
        
        pending_tasks = []
        while not self.task_queue.empty():
            try:
                task = self.task_queue.get_nowait()
                pending_tasks.append(task)
            except asyncio.QueueEmpty:
                break
        
        for task in pending_tasks:
            if await self.requeue_task_to_kafka(task):
                requeued_count += 1
            self.task_queue.task_done()
        
        async with self.requeue_lock:
            for task_id, task in list(self.in_flight_tasks):
                if await self.requeue_task_to_kafka(task):
                    requeued_count += 1
        
        self.kafka_producer.flush(timeout=10)
        
        print(f"Successfully requeued {requeued_count} urgent tasks back to Kafka")

    def add_task_to_pool(self, task):
        if not self.shutting_down:
            asyncio.run_coroutine_threadsafe(
                self.task_queue.put(task), self.loop)

    def start(self):
        if not self.running:
            self.running = True
            self.thread = Thread(target=self._run_event_loop)
            self.thread.start()
            print(f"Task pool started with {self.num_workers} workers")

    def _run_event_loop(self):
        asyncio.set_event_loop(self.loop)
        self.workers = [self.loop.create_task(
            self.worker(i)) for i in range(self.num_workers)]
        self.loop.create_task(self.continuous_task_addition())
        self.loop.run_forever()

    def stop(self):
        """Graceful shutdown with task requeueing"""
        if not self.running:
            return
            
        print("Initiating graceful shutdown of urgent task pool...")
        self.shutting_down = True
        
        if self.kafka_consumer:
            print("Stopping Kafka consumer...")
            self.kafka_consumer.close()
        
        def shutdown_sequence():
            try:
                print("Waiting for urgent workers to finish current tasks...")
                start_time = time.time()
                while time.time() - start_time < 10:
                    if self.task_queue.empty() and len(self.in_flight_tasks) == 0:
                        break
                    time.sleep(0.1)
                
                asyncio.run_coroutine_threadsafe(
                    self.requeue_all_pending_tasks(), self.loop
                ).result(timeout=15)
                
            except Exception as e:
                print(f"Error during urgent task pool shutdown sequence: {str(e)}")
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
        
        self.workers.clear()
        print("Urgent task pool stopped gracefully")

    async def continuous_task_addition(self):
        self.kafka_consumer = Consumer(self.kafka_consumer_config)
        self.kafka_consumer.subscribe([kafka_topic])
        
        print(f"Kafka consumer started for topic: {kafka_topic}")
        
        while self.running:
            try:
                msg = self.kafka_consumer.poll(1.0)
                
                if msg is None:
                    await asyncio.sleep(0.1)
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        print(f"Reached end of partition {msg.partition()}")
                    elif msg.error():
                        raise KafkaException(msg.error())
                else:
                    if self.shutting_down:
                        break
                        
                    try:
                        task_data = json.loads(msg.value().decode('utf-8'))
                        print(task_data)
                        
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
                        
                        important_tasks_queue_table_id = f"kafka_{msg.topic()}_{msg.partition()}_{msg.offset()}"
                        
                        self.add_task_to_pool((operation, important_tasks_queue_table_id, server_id, email, uuid, inbound_id,
                                               client_flow, stringLowerCaseBooleanExpressinForEnable, limitIp, totalGB, expiryTime, vpn_protocol))
                        
                        self.kafka_consumer.commit(msg)
                        
                        print(f"Added task from Kafka: {operation} for email {email}")
                        
                    except json.JSONDecodeError as e:
                        print(f"Failed to parse Kafka message: {e}")
                    except Exception as e:
                        print(f"Error processing Kafka message: {e}")
                        
            except Exception as e:
                print(f"Error in Kafka consumer: {str(e)}")
                await asyncio.sleep(1)

    

if __name__ == "__main__":
    pool = AsyncUrgentTaskPool()
    pool.start()
    print("AsyncUrgentTaskPool started automatically")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down...")
        pool.stop()