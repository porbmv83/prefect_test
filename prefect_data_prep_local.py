from prefect import Flow, Task, Parameter
from prefect.tasks.control_flow import case
from prefect.engine import signals
import saspy
import os
import time
import prefect


class RunSpreTask(Task):
    def run(self, **params):
        print(params)
        # Get the values from the global parameters
        FA_PATH = prefect.context.parameters['FA_PATH']
        RUN_INSTANCE = prefect.context.parameters['RUN_INSTANCE']
        if type(prefect.context.map_index) != int:
            MAP_INDEX = 0
        else:
            MAP_INDEX = prefect.context.map_index+1

        STAGE_INDEX = 1
        if 'STAGE_INDEX' in params:
            STAGE_INDEX = params["STAGE_INDEX"]

        # Prepare the JSON string to pass to core_run_sas. The JSON string will have all a NAME VALUE pair of each input
        list_params = []
        for key, value in params.items():
            dict_param = dict()
            dict_param["NAME"] = key
            dict_param["VALUE"] = value
            list_params.append(dict_param)

        # Create a SAS session and run the core_run_sas.sas wrapper passing the parameters
        sas = saspy.SASsession()
        sas.set_batch(True)
        sas.symput('FA_PATH', FA_PATH)
        sas.symput('RUN_INSTANCE', RUN_INSTANCE)
        sas.symput('INPUT_PARAMETERS', list_params)
        sas.symput('MAP_INDEX', MAP_INDEX)

        r = sas.submit('''
            %put INPUT_PARAMETERS: &INPUT_PARAMETERS;
            %include "&FA_PATH\prefect\source\core_run_sas.sas"; 
        ''')
        N_PARTITIONS = sas.symget('N_PARTITIONS')

        # Show the log and save it in a file
        # print(r['LOG'])
        log_name = params['NODE_CODE'] + "_" + str(STAGE_INDEX) + "_" + str(MAP_INDEX) + "_" +\
            time.strftime("%Y%m%d_%H%M%S") + '.log'
        log_file = open(os.path.join(
            RUN_INSTANCE+'\\logs', log_name), "w")
        log_file.write(r['LOG'])
        log_file.close()
        print('Check SAS Logs: ' + RUN_INSTANCE + '\\logs\\'+log_name)
        # Close the SAS session
        sas.endsas()

        # Check ig the log as any ERRORs and change the state of the task
        if (sas.check_error_log):
            print("SAS submit finished with errors")
            raise signals.FAIL()

        if type(N_PARTITIONS) != int:
            N_PARTITIONS = 0
        print("The number of partitions is: " + str(N_PARTITIONS))
        # Return the number of the partitions in case this variable was created in SAS
        return [i for i in range(N_PARTITIONS)]


class PartitionCardinalityTask(Task):
    def run(self, n_partitions):
        run_partitions_flg = 0
        if len(n_partitions) > 1:
            run_partitions_flg = 1
        print(n_partitions)
        print(run_partitions_flg)
        return run_partitions_flg


# Global parameters
BASE_DT = Parameter('BASE_DT', default='12312019')
ENTITY_ID = Parameter('ENTITY_ID', default='SASBank_1')
CYCLE_ID = Parameter('CYCLE_ID', default='10000')
FA_ID = Parameter('FA_ID', default='2022.1.1')
FA_PATH = Parameter(
    'FA_PATH', default='C:\\Development\\Temp\\Prefect_Cirrus_Core\\core\\2022.1.1')
RUN_INSTANCE = Parameter(
    'RUN_INSTANCE', default='C:\\Development\\Temp\\Prefect_Cirrus_Core\\core\\2022.1.1\\prefect\\run_instance\\prefect-spre-10000-12312019')


flow = Flow("Data_Preparation")

# Initialize and run the tasks
initialize = RunSpreTask(name='Initialize', log_stdout=True)
flow.set_dependencies(task=initialize,
                      upstream_tasks=[],
                      keyword_tasks=dict(BASE_DT=BASE_DT,
                                         CYCLE_ID=CYCLE_ID,
                                         ENTITY_ID=ENTITY_ID,
                                         FA_ID=FA_ID,
                                         FA_PATH=FA_PATH,
                                         RUN_INSTANCE=RUN_INSTANCE,
                                         NODE_CODE='core_node_init.sas',
                                         RUN_OPTION='core_cfg.run_option',
                                         SYSTEM_OPTION='sys_cfg.run_option',
                                         FLOW_OPTION='core_res.flow_option',
                                         )
                      )

filter_by_entity = RunSpreTask(name='Filter by Entity', log_stdout=True)
flow.set_dependencies(task=filter_by_entity,
                      upstream_tasks=[initialize],
                      keyword_tasks=dict(
                          GROUP_FLG='N',
                          NODE_CODE='core_node_filter_entity.sas',
                          ENTITY_IN='core_lnd.entity',
                          ENTITY_OUT='core_res.entity',
                      )
                      )

prepare_enrichment = RunSpreTask(name='Prepare Enrichment', log_stdout=True)
flow.set_dependencies(task=prepare_enrichment,
                      upstream_tasks=[initialize],
                      keyword_tasks=dict(
                          NODE_CODE='core_node_prepare_enrichment.sas',
                          EXECUTION_CONFIG='sys_cfg.execution_config',
                          ENRICHMENT_CONFIG='sys_cfg.enrichment_config',
                          ENRICHMENT_STAGE1='core_stg.enrichment_config_stage1',
                          ENRICHMENT_STAGE2='core_stg.enrichment_config_stage2',
                          ENRICHMENT_STAGE3='core_stg.enrichment_config_stage3',
                          ENRICHMENT_STAGE4='core_stg.enrichment_config_stage4',
                          ENRICHMENT_STAGE5='core_stg.enrichment_config_stage5',
                      )
                      )

# enrichment_stage_1 = RunSpreTask(name='Enrichment Stage1', log_stdout=True)
# flow.set_dependencies(task=enrichment_stage_1,
#                       upstream_tasks=[prepare_enrichment, filter_by_entity
#                                       ],
#                       keyword_tasks=dict(
#                           NODE_CODE='core_node_set_cardinality_byn.sas',
#                           ENRICHMENT_CONFIG='core_stg.enrichment_config_stage1',
#                           STAGE=1,
#                           ENRICHMENT_CARDINALITY='core_stg.enrichment_cardinality_1',
#                       )
#                       )


# map_run_partition_stage1 = RunSpreTask(
#     name='Run Partition Stage 1', log_stdout=True)
# flow.set_dependencies(task=map_run_partition_stage1,
#                       upstream_tasks=[enrichment_stage_1],
#                       keyword_tasks=dict(
#                           RUN=enrichment_stage_1,
#                       ),
#                       mapped=True,
#                       )
# flow.set_dependencies(task=map_run_partition_stage1,
#                       upstream_tasks=[enrichment_stage_1],
#                       keyword_tasks=dict(
#                           STAGE=1,
#                           NODE_CODE='core_node_run_task.sas',
#                           ENRICHMENT_CONFIG='core_stg.enrichment_config_stage_1',
#                           RUN_TYPE='CODE',
#                           FUNCTIONAL_CCY='USD',
#                           ENRICH_COUNTERPARTY_FLG='N',
#                           ENRICHMENT_CARDINALITY='core_stg.enrichment_cardinality_1',
#                           PARTITION_INPUT='core_stg.enrichment_config_stage1_part&MAP_INDEX.',
#                           RESULT_LIST_OUT='core_stg.result_list_1_&MAP_INDEX.',
#                           RUN_RESULT_OUT='core_stg.result_1_&MAP_INDEX.',
#                       )
#                       )
max_stages = 2
stages = dict()
for i in range(1, max_stages+1):
    task = dict()
    task['enrichment_stage_auto_' +
         str(i)] = RunSpreTask(name='Enrichment Stage Auto '+str(i), log_stdout=True)
    task['partition_cardinality_' + str(i)] = PartitionCardinalityTask(
        name='Check Partition Cardinality Stage '+str(i), log_stdout=True)
    task['map_run_partition_auto_stage_' +
         str(i)] = RunSpreTask(name='Run Partition Auto Stage '+str(i), log_stdout=True)
    stages['stage' + str(i)] = task

previous_key = None
for stage_index, (stage_key, stage_value) in enumerate(stages.items()):

    task_list = list(stage_value.keys())
    enrichment_stage_auto_n = stage_value[task_list[0]]
    partition_cardinality_n = stage_value[task_list[1]]
    map_run_partition_auto_stage_n = stage_value[task_list[2]]

    if previous_key is None:
        list_upstream_tasks = [prepare_enrichment, filter_by_entity]
    else:
        list_upstream_tasks = [previous_key]

    flow.set_dependencies(task=enrichment_stage_auto_n,
                          upstream_tasks=list_upstream_tasks,
                          keyword_tasks=dict(
                              NODE_CODE='core_node_set_cardinality_byn.sas',
                              ENRICHMENT_CONFIG='core_stg.enrichment_config_stage' +
                              str(stage_index+1),
                              STAGE=stage_index+1,
                              ENRICHMENT_CARDINALITY='core_stg.enrichment_cardinality_' +
                              str(stage_index+1),
                          )
                          )

    flow.set_dependencies(task=partition_cardinality_n,
                          upstream_tasks=[enrichment_stage_auto_n],
                          keyword_tasks=dict(n_partitions=enrichment_stage_auto_n
                                             )
                          )

    with case(partition_cardinality_n, 1):
        flow.set_dependencies(task=map_run_partition_auto_stage_n,
                              upstream_tasks=[enrichment_stage_auto_n],
                              keyword_tasks=dict(
                                  RUN=enrichment_stage_auto_n,
                              ),
                              mapped=True,
                              )
        flow.set_dependencies(task=map_run_partition_auto_stage_n,
                              upstream_tasks=[partition_cardinality_n],
                              keyword_tasks=dict(
                                  STAGE=stage_index+1,
                                  NODE_CODE='core_node_run_task.sas',
                                  ENRICHMENT_CONFIG='core_stg.enrichment_config_stage_' +
                                  str(stage_index+1),
                                  RUN_TYPE='CODE',
                                  FUNCTIONAL_CCY='USD',
                                  ENRICH_COUNTERPARTY_FLG='N',
                                  ENRICHMENT_CARDINALITY='core_stg.enrichment_cardinality_' +
                                  str(stage_index+1),
                                  PARTITION_INPUT='core_stg.enrichment_config_stage' +
                                  str(stage_index+1)+'_part&MAP_INDEX.',
                                  RESULT_LIST_OUT='core_stg.result_list_' +
                                  str(stage_index+1)+'_&MAP_INDEX.',
                                  RUN_RESULT_OUT='core_stg.result_' +
                                  str(stage_index+1)+'_&MAP_INDEX.',
                              )
                              )

    previous_key = map_run_partition_auto_stage_n


flow.visualize()
flow.run()
