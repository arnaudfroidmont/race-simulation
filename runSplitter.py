import os, json, pika, random, threading
from datetime import datetime
import numpy as np
from io import StringIO
import math, copy
import ast

class splitter:
    channel_consumer_splittable = ""
    channel_consumer_parent = ""
    channel_producer = ""
    channel_producer_final = ""
    connection1 = ""
    connection2 = ""
    connection3 = ""
    total_tasks = 0
    scenario_id = ""
    strategy_id = ""
    parent_result = ""
    callback_count = 0
    current_runs = 0
    average_position = 0.0


    def callback_parent(self, ch, method, properties, body):
        strategy_results = ast.literal_eval(body.decode('utf-8'))
        self.callback_count = self.callback_count + 1
        if (self.parent_result == ""):
            self.parent_result = {'strategy':strategy_results['strategy'],'level':1}
            self.average_position = strategy_results['results']
            self.current_runs = strategy_results['no_sim_runs']
        else:
            self.average_position = ( self.current_runs * self.average_position + strategy_results['no_sim_runs'] * strategy_results['results'] ) / (self.current_runs + strategy_results['no_sim_runs'])
            self.current_runs = (self.current_runs + strategy_results['no_sim_runs'])
        if self.callback_count == self.total_tasks :
            self.parent_result['results'] = self.average_position
            self.parent_result['no_sim_runs'] = self.current_runs
            self.connection3 = pika.BlockingConnection(pika.ConnectionParameters(os.environ['RABBITMQ_HOST']))
            self.channel_producer_final = self.connection3.channel()
            _queue_final = str(self.scenario_id) + "_result"
            self.channel_producer_final.queue_declare(queue=_queue_final)
            self.channel_producer_final.basic_publish(exchange='', routing_key=_queue_final, body=str(self.parent_result))
            _queue_delete = str(self.scenario_id) + "_" + str(self.strategy_id)
            self.channel_consumer_parent.queue_delete(queue=_queue_delete)
            self.total_tasks = 0
            self.callback_count = 0
            self.average_position = 0.0
            self.parent_result = ""
            self.connection3.close() 
   

    def callback(self, ch, method, properties, body):
        self.connection2 = pika.BlockingConnection(pika.ConnectionParameters(os.environ['RABBITMQ_HOST']))
        self.channel_consumer_parent = self.connection2.channel()
        self.channel_producer = self.connection2.channel()
        self.channel_producer.queue_declare(queue=os.environ['RABBITMQ_TASKQUEUE'], arguments={"x-max-priority": 10})
        count = 0
        strategy =ast.literal_eval(body.decode('utf-8'))
        sim_opts_ = strategy['sim_opts']

        self.scenario_id = strategy["scenario_id"]
        self.strategy_id = strategy["strategy_id"]

        childs=math.ceil(float(sim_opts_["no_sim_runs"])/float(os.environ['NUM_STEPS']))
        for n in range(childs):
            
            child_strategy = copy.deepcopy(strategy)
            count = count + 1
            child_strategy["child_id"] = count
            child_strategy["level"] = 2
            child_strategy["sim_opts"]["no_sim_runs"] = int(os.environ['NUM_STEPS'])
            print(child_strategy["sim_opts"])
            self.channel_producer.basic_publish(
                properties=pika.BasicProperties(priority=9),
                exchange='', 
                routing_key=os.environ['RABBITMQ_TASKQUEUE'], 
                body=str(child_strategy))
        self.total_tasks = count
        count = 0
        _queue = str(self.scenario_id) + "_" + str(self.strategy_id)
        self.channel_consumer_parent.queue_declare(queue=_queue)
        self.channel_consumer_parent.basic_qos(prefetch_count=int(os.environ['RABBITMQ_PREFETCHCOUNT']))
        self.channel_consumer_parent.basic_consume(queue=_queue, auto_ack=True, on_message_callback=self.callback_parent)
        self.channel_consumer_parent.start_consuming()
        self.connection2.close()

    def __init__(self):
        while True:
            self.connection1 = pika.BlockingConnection(pika.ConnectionParameters(os.environ['RABBITMQ_HOST']))
            self.channel_consumer_splittable = self.connection1.channel()
            self.channel_consumer_splittable.queue_declare(queue=os.environ['RABBITMQ_TASKQUEUE_SPLITTABLE'])
            self.channel_consumer_splittable.basic_qos(prefetch_count=int(os.environ['RABBITMQ_PREFETCHCOUNT']))
            self.channel_consumer_splittable.basic_consume(queue=os.environ['RABBITMQ_TASKQUEUE_SPLITTABLE'], auto_ack=True, on_message_callback=self.callback)
            self.channel_consumer_splittable.start_consuming()


splitter()
