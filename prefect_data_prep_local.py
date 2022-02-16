from prefect import Flow, Task, Parameter
import saspy
import os
import time


class RunSpreTask(Task):
    def run(self, args):
        # Collecting the parameters needed from the list of dictionaries
        dict_fa_path = next(
            (item for item in args if item['NAME'] == 'FA_PATH'), None)
        dict_run_instance = next(
            (item for item in args if item['NAME'] == 'RUN_INSTANCE'), None)
        dict_node_code = next(
            (item for item in args if item['NAME'] == 'NODE_CODE'), None)

        # Create a SAS session and run the core_run_sas.sas wrapper passing the parameters
        sas = saspy.SASsession()
        sas.symput('FA_PATH', dict_fa_path['VALUE'])
        sas.symput('INPUT_PARAMETERS', args)
        r = sas.submit('''
			%put : INPUT_PARAMETERS: &INPUT_PARAMETERS;
			%include "&FA_PATH\prefect\source\core_run_sas.sas"; 
		''')

        # Show the log and save it in a file
        print(r['LOG'])
        log_name = dict_node_code['VALUE'] + "_" + \
            time.strftime("%Y%m%d_%H%M%S") + '.log'
        log_file = open(os.path.join(
            dict_run_instance['VALUE']+'\\logs', log_name), "w")
        log_file.write(r['LOG'])
        log_file.close()

        # Close the SAS session
        sas.endsas()


def processParameters(**args):
    # Function to create a list of dict pair name and value
    list_args = []
    for key, value in args.items():
        dict_arg = dict()
        dict_arg["NAME"] = key
        dict_arg["VALUE"] = value
        list_args.append(dict_arg)

    return list_args


# Instantiate the flow and the task
initialize = RunSpreTask(name='Initialize', log_stdout=True)

with Flow("prefect-spre-dataPrep") as flow:

    # Global parameters
    BASE_DT = Parameter('BASE_DT', default='12312019')
    ENTITY_ID = Parameter('ENTITY_ID', default='SASBank_1')
    CYCLE_ID = Parameter('CYCLE_ID', default='10000')
    FA_ID = Parameter('FA_ID', default='2022.1.1')
    FA_PATH = 'C:\\Development\\Temp\\Prefect_Cirrus_Core\\core\\' + FA_ID + '\\'
    RUN_INSTANCE = FA_PATH+'prefect\\run_instance\\' + \
        'prefect-spre-'+CYCLE_ID+'-'+BASE_DT

    # Initialize and run the task
    args = processParameters(BASE_DT=BASE_DT,
                             CYCLE_ID=CYCLE_ID,
                             ENTITY_ID=ENTITY_ID,
                             FA_ID=FA_ID,
                             FA_PATH=FA_PATH,
                             RUN_INSTANCE=RUN_INSTANCE,
                             NODE_CODE='core_node_init',
                             RUN_OPTION='core_cfg.run_option',
                             SYSTEM_OPTION='sys_cfg.run_option',
                             FLOW_OPTION='core_res.flow_option'
                             )
    initialize(args)

flow.run()
