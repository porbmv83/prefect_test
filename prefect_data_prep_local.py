from prefect import Flow, Task, Parameter
import saspy
import os
import time
import prefect


class RunSpreTask(Task):
    def run(self, **params):
        # Get the values from the global parameters
        FA_PATH = prefect.context.parameters['FA_PATH']
        RUN_INSTANCE = prefect.context.parameters['RUN_INSTANCE']

        # Prepare the JSON string to pass to core_run_sas
        list_params = []
        for key, value in params.items():
            dict_param = dict()
            dict_param["NAME"] = key
            dict_param["VALUE"] = value
            list_params.append(dict_param)

        # Create a SAS session and run the core_run_sas.sas wrapper passing the parameters
        sas = saspy.SASsession()
        sas.symput('FA_PATH', FA_PATH)
        sas.symput('RUN_INSTANCE', RUN_INSTANCE)
        sas.symput('INPUT_PARAMETERS', list_params)
        r = sas.submit('''
			%put INPUT_PARAMETERS: &INPUT_PARAMETERS;
			%include "&FA_PATH\prefect\source\core_run_sas.sas"; 
		''')

        # Show the log and save it in a file
        print(r['LOG'])
        log_name = params['NODE_CODE'] + "_" + \
            time.strftime("%Y%m%d_%H%M%S") + '.log'
        log_file = open(os.path.join(
            RUN_INSTANCE+'\\logs', log_name), "w")
        log_file.write(r['LOG'])
        log_file.close()

        # Close the SAS session
        sas.endsas()


# Instantiate the flow and the task
initialize = RunSpreTask(name='Initialize', log_stdout=True)
initialize2 = RunSpreTask(name='Initialize2', log_stdout=True)
flow = Flow("prefect-spre-dataPrep")

# Global parameters
BASE_DT = Parameter('BASE_DT', default='12312019')
ENTITY_ID = Parameter('ENTITY_ID', default='SASBank_1')
CYCLE_ID = Parameter('CYCLE_ID', default='10000')
FA_ID = Parameter('FA_ID', default='2022.1.1')
FA_PATH = Parameter(
    'FA_PATH', default='C:\\Development\\Temp\\Prefect_Cirrus_Core\\core\\2022.1.1\\')
RUN_INSTANCE = Parameter(
    'RUN_INSTANCE', default='C:\\Development\\Temp\\Prefect_Cirrus_Core\\core\\2022.1.1\\prefect\\run_instance\\prefect-spre-10000-12312019')

# Initialize and run the task
flow.set_dependencies(task=initialize,
                      upstream_tasks=[],
                      keyword_tasks=dict(BASE_DT=BASE_DT,
                                         CYCLE_ID=CYCLE_ID,
                                         ENTITY_ID=ENTITY_ID,
                                         FA_ID=FA_ID,
                                         FA_PATH=FA_PATH,
                                         RUN_INSTANCE=RUN_INSTANCE,
                                         NODE_CODE='core_node_init',
                                         RUN_OPTION='core_cfg.run_option',
                                         SYSTEM_OPTION='sys_cfg.run_option',
                                         FLOW_OPTION='core_res.flow_option')
                      )
flow.set_dependencies(task=initialize2,
                      upstream_tasks=[initialize],
                      keyword_tasks=dict(
                          NODE_CODE='core_node_init2',
                          FLOW_OPTION='core_res.flow_option')
                      )

flow.visualize()
flow.run()
