import os, pika, json, threading
from datetime import datetime
from random import randrange
import copy, ast

class client:
    total_tasks = 0
    scenario_id = randrange(1000000,10000000,1)
    callback_count = 0
    portfolio_result = ""
    channel_producer = ""
    channel_producer_splittable = ""
    channel_consumer = ""
    connection = ""
    start_time = ""


    def callback(self, ch, method, properties, body):
        if (self.callback_count <= self.total_tasks):
            self.callback_count = self.callback_count + 1
            print("TASKS RECEIVED: " + str(self.callback_count))
            results =ast.literal_eval(body.decode('utf-8'))
            print(results['strategy']['strategy'],results['results'])

            if (self.callback_count == self.total_tasks):
                end_time = datetime.now()
                total_time = end_time - self.start_time
                print("TOTAL TIME: " + str(total_time))
                _queue = str(self.scenario_id) + "_result"
                self.channel_consumer.queue_delete(queue=_queue)
                self.channel_consumer.stop_consuming()

    def __init__(self):
        ## MAIN
        
        fixed_param = { "driver" : "VER",
                        "initial_compound" : "A3" , 
                        "race_pars_file" : "pars_Spielberg_2019.ini" }

        strategy_pars = { "strategies" : 
                            [   {   "stop_num": 1, 
                                "laps": [range(35,40)],
                                "compounds": [["A4","A6"]]
                                #"compounds": [["A4"]]
                            }#,
                             #   {   "stop_num": 2, 
                             #   "laps": [range(20,21),range(40,42)],
                             #   "compounds": [["A3","A4","A6"],["A3","A4","A6"]]
                            #}
                            ]
                        }
        sim_opts_ = {   "use_prob_infl": True,
                        "create_rand_events": True,
                        "use_vse": False,
                        "no_sim_runs": 500,
                        "no_workers": 1,
                        "use_print": False,
                        "use_print_result": False,
                        "use_plot": False    }
        index = 0
        mc_strategies=[]
        for strategy in strategy_pars[ "strategies" ] :
            tyre_changes_init=[[0, fixed_param["initial_compound"], 2, 0.0]]
            tyre_changes=copy.deepcopy(tyre_changes_init)
            if strategy["stop_num"] == 1:
                for lap in strategy["laps"][0]:
                    for compound in strategy["compounds"][0]:
                        tyre_changes.append([lap,compound, 0, 0.0])
                        mc_strategies.append({"Input_file":fixed_param["race_pars_file"],"driver":fixed_param["driver"],"strategy":tyre_changes})
                        index = index + 1
                        tyre_changes=copy.deepcopy(tyre_changes_init)
            if strategy["stop_num"] == 2:
                for lap in strategy["laps"][0]:
                    for compound in strategy["compounds"][0]:
                        tyre_changes.append([lap,compound, 0, 0.0])
                        for lap2 in strategy["laps"][1]:
                            for compound2 in strategy["compounds"][1]:
                                tyre_changes.append([lap2,compound2, 0, 0.0])
                                mc_strategies.append({"Input_file":fixed_param["race_pars_file"],"driver":fixed_param["driver"],"strategy":tyre_changes})
                                index = index + 1
                                tyre_changes=copy.deepcopy(tyre_changes_init)
                    
        ## Reading simulations JSON file
        print("SCENARIO_ID: " + str(self.scenario_id))
        self.start_time = datetime.now()
        print("START TIME: " + str(self.start_time))
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(os.environ['RABBITMQ_HOST']))
        self.channel_producer = self.connection.channel()
        self.channel_producer.queue_declare(queue=os.environ['RABBITMQ_TASKQUEUE'], arguments={"x-max-priority": 10})
        self.channel_producer_splittable = self.connection.channel()
        self.channel_producer_splittable.queue_declare(queue=os.environ['RABBITMQ_TASKQUEUE_SPLITTABLE'])
        
        
        for mc_strategy in mc_strategies:
            self.total_tasks = self.total_tasks + 1
            #submit_time = {"submit_time": str(datetime.now())}
            mc_strategy["sim_opts"]=sim_opts_
            mc_strategy["level"]=1
            mc_strategy["scenario_id"]=self.scenario_id
            mc_strategy["strategy_id"]= randrange(1000000,10000000,1)

            if mc_strategy["sim_opts"]["no_sim_runs"] > int(os.environ['NUM_STEPS']):
                self.channel_producer_splittable.basic_publish(
                    exchange='', 
                    routing_key=os.environ['RABBITMQ_TASKQUEUE_SPLITTABLE'], 
                    body=str(mc_strategy))
            else:
                self.channel_producer.basic_publish(
                    exchange='', 
                    routing_key=os.environ['RABBITMQ_TASKQUEUE'], 
                    body=str(mc_strategy))


        aux = datetime.now()
        send_time = aux - self.start_time
        print("SEND TIME: " + str(send_time))

#        self.connection = pika.BlockingConnection(pika.ConnectionParameters(os.environ['RABBITMQ_HOST']))
        self.channel_consumer = self.connection.channel() 
        _queue = str(self.scenario_id) + "_result"
        self.channel_consumer.queue_declare(queue=_queue)
        self.channel_consumer.basic_qos(prefetch_count=500)
        self.channel_consumer.basic_consume(queue=_queue, auto_ack=True, on_message_callback=self.callback)
        self.channel_consumer.start_consuming()

client()
