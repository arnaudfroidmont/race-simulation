
import racesim
import helper_funcs
from racesim.src.race_handle import race_handle
from concurrent import futures  # required for parallel computing
import numpy as np
import time
import os
import pkg_resources
import pickle
import pika, ast, threading, functools

"""
author:
Alexander Heilmeier

date:
12.07.2018

.. description::
This file includes the main function as well as required plot functions. The script part required to run
the simulation is located at the bottom. Have a look there to insert the required user parameters.
"""

# ----------------------------------------------------------------------------------------------------------------------
# CHECK PYTHON DEPENDENCIES --------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------

# get repo path
repo_path_ = os.path.dirname(os.path.abspath(__file__))

# read dependencies from requirements.txt
requirements_path = os.path.join(repo_path_, 'requirements.txt')
dependencies = []

with open(requirements_path, 'r') as fh_:
    line = fh_.readline()

    while line:
        dependencies.append(line.rstrip())
        line = fh_.readline()

# check dependencies
# pkg_resources.require(dependencies)


# ----------------------------------------------------------------------------------------------------------------------
# MAIN FUNCTION --------------------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------

def main(sim_opts: dict, race_pars_file: str, mcs_pars_file: str, mcs_driver: str) -> list:

    # ------------------------------------------------------------------------------------------------------------------
    # INITIALIZATION ---------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------

    # get repo path
    repo_path = os.path.dirname(os.path.abspath(__file__))

    # create output folders (if not existing)
    output_path = os.path.join(repo_path, "racesim", "output")

    results_path = os.path.join(output_path, "results")
    os.makedirs(results_path, exist_ok=True)

    invalid_dumps_path = os.path.join(output_path, "invalid_dumps")
    os.makedirs(invalid_dumps_path, exist_ok=True)

    testobjects_path = os.path.join(output_path, "testobjects")
    os.makedirs(testobjects_path, exist_ok=True)

    # load parameters
    overwrite_dict=None
    pars_in, vse_paths = racesim.src.import_pars.import_pars(use_print=sim_opts["use_print"],
                                                             use_vse=sim_opts["use_vse"],
                                                             race_pars_file=race_pars_file,
                                                             mcs_pars_file=mcs_pars_file,
                                                             overwrite_dict=overwrite_dict)

    # check parameters
    racesim.src.check_pars.check_pars(sim_opts=sim_opts, pars_in=pars_in)

    # ------------------------------------------------------------------------------------------------------------------
    # SIMULATION -------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------

    # create list containing the simulated race object (single run) or dicts with valid results (multiple runs)
    race_results = []

    # save start time for runtime calculation
    if sim_opts["use_print"]:
        print("INFO: Starting simulations...")
    t_start = time.perf_counter()
    # iteration variables
    no_sim_runs_left = sim_opts["no_sim_runs"]  # counter for the number of races left for simulation
    ctr_invalid = 0                             # counter for the number of simulated races marked as invalid

    # SINGLE PROCESS ---------------------------------------------------------------------------------------------------
    if sim_opts["no_workers"] == 1:

        while no_sim_runs_left > 0:
            # simulate race
            tmp_race_handle = race_handle(pars_in=pars_in,
                                          use_prob_infl=sim_opts['use_prob_infl'],
                                          create_rand_events=sim_opts['create_rand_events'],
                                          vse_paths=vse_paths)
            no_sim_runs_left -= 1

            # CASE 1: result is valid
            if tmp_race_handle.result_status == 0:
                # save race object for later evaluation (single race) or simple race results (MCS)
                if sim_opts["no_sim_runs"] > 1:
                    race_results.append(tmp_race_handle.get_race_results())
                else:
                    race_results.append(tmp_race_handle)

            # CASE 2: result is invalid
            else:
                # increase no_sim_runs_left
                ctr_invalid += 1
                no_sim_runs_left += 1

                # pickle race object for further analysis
                if tmp_race_handle.result_status >= 10 or tmp_race_handle.result_status == -1:
                    cur_time_str = time.strftime("%Y%m%d_%H%M%S")
                    tmp_file_path = os.path.join(invalid_dumps_path, cur_time_str + "_invalid_race_%i_%i.pkl"
                                                 % (ctr_invalid, tmp_race_handle.result_status))

                    with open(tmp_file_path, 'wb') as fh:
                        pickle.dump(tmp_race_handle, fh)

            # print progressbar
            if sim_opts["use_print"]:
                helper_funcs.src.progressbar.progressbar(i=sim_opts["no_sim_runs"] - no_sim_runs_left,
                                                         i_total=sim_opts["no_sim_runs"],
                                                         prefix="INFO: Simulation progress:")

    # MULTIPLE PROCESSES -----------------------------------------------------------------------------------------------
    else:
        # set maximum number of jobs in the waiting queue at the same time -> limits RAM usage
        max_no_concurrent_jobs = 200

        # create executor instance (pool of processes available for parallel calculations)
        with futures.ProcessPoolExecutor(max_workers=sim_opts["no_workers"]) as executor:

            while no_sim_runs_left > 0:
                # reset job queue (list containing current simulation jobs)
                job_queue = []

                # submit simulations to the waiting queue of the executor instance as long as we have races left for
                # simulation and the job queue is not full
                while len(job_queue) <= max_no_concurrent_jobs and no_sim_runs_left > 0:
                    job_queue.append(executor.submit(race_handle,
                                                     pars_in,
                                                     sim_opts['use_prob_infl'],
                                                     sim_opts['create_rand_events'],
                                                     vse_paths))
                    no_sim_runs_left -= 1

                # collect results as soon as they are available
                for job_handle in futures.as_completed(job_queue):
                    tmp_race_handle = job_handle.result()

                    # CASE 1: result is valid
                    if tmp_race_handle.result_status == 0:
                        # save race object for later evaluation (single race) or simple race results (MCS)
                        if sim_opts["no_sim_runs"] > 1:
                            race_results.append(tmp_race_handle.get_race_results())
                        else:
                            race_results.append(tmp_race_handle)

                    # CASE 2: result is invalid
                    else:
                        # increase no_sim_runs_left
                        ctr_invalid += 1
                        no_sim_runs_left += 1

                        # pickle race object for further analysis
                        if tmp_race_handle.result_status >= 10 or tmp_race_handle.result_status == -1:
                            cur_time_str = time.strftime("%Y%m%d_%H%M%S")
                            tmp_file_path = os.path.join(invalid_dumps_path, cur_time_str + "_invalid_race_%i_%i.pkl"
                                                         % (ctr_invalid, tmp_race_handle.result_status))

                            with open(tmp_file_path, 'wb') as fh:
                                pickle.dump(tmp_race_handle, fh)

                # print progressbar
                if sim_opts["use_print"]:
                    helper_funcs.src.progressbar.progressbar(i=sim_opts["no_sim_runs"] - no_sim_runs_left,
                                                             i_total=sim_opts["no_sim_runs"],
                                                             prefix="INFO: Simulation progress:")

    # print number of invalid races
    if sim_opts["use_print"]:
        print("INFO: There were %i invalid races!" % ctr_invalid)

    # print runtime into console window
    if sim_opts["use_print"]:
        runtime = time.perf_counter() - t_start
        print("INFO: Simulation runtime: {:.3f}s ({:.3f}ms per race)".format(runtime,
                                                                             runtime / sim_opts["no_sim_runs"] * 1000))

    # ------------------------------------------------------------------------------------------------------------------
    # POSTPROCESSING ---------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------

    if sim_opts["use_print"]:
        print("INFO: Postprocessing in progress...")

    # SINGLE RACE ------------------------------------------------------------------------------------------------------
    if sim_opts["no_sim_runs"] == 1:
        race_results[0].check_valid_result()

        if sim_opts["use_print_result"]:
            race_results[0].print_result()
            # race_results[0].print_details()

        if sim_opts["use_plot"]:
            # race_results[0].plot_laptimes()
            # race_results[0].plot_positions()
            # race_results[0].plot_racetime_diffto_refdriver(1)
            # race_results[0].plot_raceprogress_over_racetime()

            laps_simulated = race_results[0].cur_lap
            t_race_winner = np.sort(race_results[0].racetimes[laps_simulated, :])[0]
            race_results[0].plot_racetime_diffto_reflaptime(ref_laptime=t_race_winner / laps_simulated)

        # evaluation
        # race_results[0].print_race_standings(racetime=2520.2)

        # save lap times, race times and positions to csv files
        race_results[0].export_results_as_csv(results_path=results_path)

        # pickle race object for possible CI testing
        result_objects_file_path = os.path.join(testobjects_path, "testobj_racesim_%s_%i.pkl"
                                                % (pars_in["track_pars"]["name"], pars_in["race_pars"]["season"]))
        with open(result_objects_file_path, 'wb') as fh:
            pickle.dump(race_results[0], fh)

    # MULTIPLE RACES ---------------------------------------------------------------------------------------------------
    else:
        # plot histograms
        mean_pos = racesim.src.mcs_analysis.mcs_analysis(race_results=race_results,
                                              use_print_result=sim_opts["use_print_result"],
                                              use_plot=sim_opts["use_plot"],
                                              mcs_driver=mcs_driver)
    if sim_opts["use_print"]:
        print("INFO: Simulation finished successfully!")
        
    return mean_pos  # return required in case of CI testing


# ----------------------------------------------------------------------------------------------------------------------
# MAIN FUNCTION CALL ---------------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------
# def callback(ch, method, properties, body):
#     strategy =ast.literal_eval(body.decode('utf-8')) 
#     race_pars_file_= strategy['Input_file']
#     sim_opts_ = {"use_prob_infl": True,
#                 "create_rand_events": True,
#                 "use_vse": False,
#                 "no_sim_runs": 1000,
#                 "no_workers": 1,
#                 "use_print": True,
#                 "use_print_result": True,
#                 "use_plot": True}
#     mcs_pars_file_ = 'pars_mcs.ini'
#     print(strategy["strategy"])
#     main(sim_opts=sim_opts_,
#          race_pars_file=race_pars_file_,
#          mcs_pars_file=mcs_pars_file_)

### Arnaud OLD
# def ack_message(ch, delivery_tag):
#     """Note that `ch` must be the same pika channel instance via which
#     the message being ACKed was retrieved (AMQP protocol constraint).
#     """
#     if ch.is_open:
#         ch.basic_ack(delivery_tag)
#     else:
#         # Channel is already closed, so we can't ACK this message;
#         # log and/or do something that makes sense for your app in this case.
#         pass

# def do_work(conn, ch, delivery_tag, body):
#     thread_id = threading.get_ident()
#     strategy =ast.literal_eval(body.decode('utf-8'))
#     race_pars_file_= strategy['Input_file']
#     sim_opts_ = strategy['sim_opts']
#     mcs_pars_file_ = 'pars_mcs.ini'
#     results = main(sim_opts=sim_opts_,
#          race_pars_file=race_pars_file_,
#          mcs_pars_file=mcs_pars_file_,
#          mcs_driver=strategy['driver'])
#     cb = functools.partial(ack_message, ch, delivery_tag)
#     conn.add_callback_threadsafe(cb) 
#     connection2 = pika.BlockingConnection(pika.ConnectionParameters(os.environ['RABBITMQ_HOST']))
#     channel_producer = self.connection.channel() 
#     _queue = str(strategy[]) + "_result"
#     channel_producer.queue_declare(queue=_queue)

# def on_message(ch, method_frame, _header_frame, body, args):
#     (conn, thrds) = args
#     delivery_tag = method_frame.delivery_tag
#     t = threading.Thread(target=do_work, args=(conn, ch, delivery_tag, body))
#     t.start()
#     thrds.append(t)
#     t.join()

### Arnaud OLD



def callback(ch, method, properties, body):
    strategy = ast.literal_eval(body.decode('utf-8'))
    race_pars_file_= strategy['Input_file']
    sim_opts_ = strategy['sim_opts']
    mcs_pars_file_ = 'pars_mcs.ini'
    results = main(sim_opts=sim_opts_,
        race_pars_file=race_pars_file_,
        mcs_pars_file=mcs_pars_file_,
        mcs_driver=strategy['driver'])
    
    body = {'strategy':strategy,'results':results,'level':strategy['level'],'no_sim_runs':sim_opts_["no_sim_runs"]}
    if strategy["level"] == 1:
        _queue = str(mc_strategy["scenario_id"]) + "_result"
    else: 
        _queue = str(strategy["scenario_id"]) + "_" + str(strategy["strategy_id"])

    channel_producer.queue_declare(queue=_queue)
    channel_producer.basic_publish(exchange='', routing_key=_queue, body=str(body))

#                    _queue = str(_portfolio_id) + "_" + str(_parent_id)
#                    self.channel_producer.queue_declare(queue=_queue)
#                    self.channel_producer.basic_publish(exchange='', routing_key=_queue, body=json.dumps(result))


if __name__ == '__main__':

    # ------------------------------------------------------------------------------------------------------------------
    # USER INPUT -------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------

    # set race parameter file names
#    race_pars_file_ = 'pars_Spielberg_2019_mc.ini'
    connection = pika.BlockingConnection(pika.ConnectionParameters(os.environ['RABBITMQ_HOST']))
    channel_consumer = connection.channel()
    channel_producer = connection.channel()
    channel_consumer.queue_declare(queue=os.environ['RABBITMQ_TASKQUEUE'], arguments={"x-max-priority": 10})
    channel_consumer.basic_qos(prefetch_count=int(os.environ['RABBITMQ_PREFETCHCOUNT']))
    channel_consumer.basic_consume(queue=os.environ['RABBITMQ_TASKQUEUE'], auto_ack=True, on_message_callback=callback)
    channel_consumer.start_consuming()
### Arnaud OLD
    # connection = pika.BlockingConnection(pika.ConnectionParameters(host=os.environ['RABBITMQ_HOST']))
    # channel = connection.channel()
    # threads = []
    # on_message_callback = functools.partial(on_message, args=(connection, threads))
    # channel.basic_consume(queue=os.environ['RABBITMQ_TASKQUEUE'], on_message_callback=on_message_callback)
    # channel.start_consuming()
    
    # for thread in threads:
    #     thread.join()
### Arnaud OLD






    # set simulation options
    # use_prob_infl:        activates probabilistic influences within the race simulation -> lap times, pit stop
    #                       durations, race start performance
    # create_rand_events:   activates the random creation of FCY (full course yellow) phases and retirements in the race
    #                       simulation -> they will only be created if the according entries in the parameter file
    #                       contain empty lists, otherwise the file entries are used
    # use_vse:              determines if the VSE (virtual strategy engineer) is used to take tire change decisions
    #                       -> the VSE type is defined in the parameter file (VSE_PARS)
    # no_sim_runs:          number of (valid) races to simulate
    # no_workers:           defines number of workers for multiprocess calculations, 1 for single process, >1 for
    #                       multi-process (you can use print(multiprocessing.cpu_count()) to determine the max. number)
    # use_print:            set if prints to console should be used or not (does not suppress hints/warnings)
    # use_print_result:     set if result should be printed to console or not
    # use_plot:             set if plotting should be used or not


    # ------------------------------------------------------------------------------------------------------------------
    # SIMULATION CALL --------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------

    


# if use_print:
#         print("INFO: Loading race parameters...")
#     par_file_path = os.path.join(repo_path, "racesim", "input", "parameters", race_pars_file)

#     parser = configparser.ConfigParser()
#     pars_in = {}

#     if not parser.read(par_file_path):
#         raise RuntimeError('Specified race parameter config file does not exist or is empty!')

#     pars_in["race_pars"] = json.loads(parser.get('RACE_PARS', 'race_pars'))
#     pars_in["monte_carlo_pars"] = json.loads(parser.get('MONTE_CARLO_PARS', 'monte_carlo_pars'))
#     pars_in["track_pars"] = json.loads(parser.get('TRACK_PARS', 'track_pars'))
#     pars_in["car_pars"] = json.loads(parser.get('CAR_PARS', 'car_pars'))
#     pars_in["tireset_pars"] = json.loads(parser.get('TIRESET_PARS', 'tireset_pars'))
#     pars_in["driver_pars"] = json.loads(parser.get('DRIVER_PARS', 'driver_pars'))
#     pars_in["event_pars"] = json.loads(parser.get('EVENT_PARS', 'event_pars'))
#     pars_in["vse_pars"] = json.loads(parser.get('VSE_PARS', 'vse_pars'))
