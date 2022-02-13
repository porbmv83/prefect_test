from prefect import task, Flow
from prefect.executors import DaskExecutor
from prefect.run_configs import KubernetesRun
from dask_kubernetes import make_pod_spec
#from dask_kubernetes import KubeCluster
from prefect.storage import GitHub
import saspy

FLOW_NAME = "dask_spre"
STORAGE = GitHub(
    repo="porbmv83/prefect_test",
    path=f"{FLOW_NAME}.py",
    # access_token_secret="GITHUB_ACCESS_TOKEN",   required with private repositories
)

POD_SPEC = make_pod_spec(
    image="sasporbmvacr.azurecr.io/prefect-dask-spre:latest",
    memory_limit="2G",
    cpu_limit=1,
    memory_request="2G",
    cpu_request=1,
    #    env={"EXTRA_PIP_PACKAGES": ""},
    extra_container_config={"volumeMounts": [{"name": "core",
                                             "mountPath": "/core"}]},
    extra_pod_config={"volumes": [{"name": "core",
                                   "persistentVolumeClaim": {"claimName": "sas-risk-cirrus-core-pvc"}}],
                      "imagePullSecrets": [{"name": "sasporbmvacr-image-pull-secret"}],
                      "nodeSelector": {"workload.sas.com/class": "compute"},
                      "tolerations": [{"effect": "NoSchedule",
                                       "key": "workload.sas.com/class",
                                       "operator": "Equal",
                                       "value": "compute"}]
                      },
)

EXECUTOR = DaskExecutor(
    cluster_class="dask_kubernetes.KubeCluster",
    cluster_kwargs={"pod_template": POD_SPEC,
                    "name": "dask-spre",
                    },
    adapt_kwargs={"minimum": 1, "maximum": 4,  "work_stealing": "True"},
)

RUN_CONFIG = KubernetesRun(
    image="prefecthq/prefect:latest-python3.8",
    env={"EXTRA_PIP_PACKAGES": "dask-kubernetes saspy"},
    labels=["porbmv"],
)


@task(log_stdout=True)
def inc(x):
    sas = saspy.SASsession()
    sas.symput('sas_x', x)
    r = sas.submit("""
		%put INC: Python value is: &sas_x;
		data _null_;
			sas_z = &sas_x+1;
			call symput('sas_z', sas_z);
		run; 
	""")
    z = sas.symget('sas_z')
    print(r['LOG'])
    sas.endsas()
    print('INC: SAS value is: ' + str(z))
    return z


@task(log_stdout=True)
def dec(x):
    sas = saspy.SASsession()
    sas.symput('sas_x', x)
    r = sas.submit("""
		%put DEC: Python value is: &sas_x;
		data _null_;
			sas_z = &sas_x-1;
			call symput('sas_z', sas_z);
		run; 
	""")
    z = sas.symget('sas_z')
    print(r['LOG'])
    sas.endsas()
    print('DEC: SAS value is: ' + str(z))
    return z


@task(log_stdout=True)
def add(x, y):
    sas = saspy.SASsession()
    sas.symput('sas_x', x)
    sas.symput('sas_y', y)
    r = sas.submit("""
		%put ADD: Python value is: &sas_x and &sas_y;
		data _null_;
			sas_z = &sas_x-&sas_y;
			call symput('sas_z', sas_z);
		run; 
	""")
    z = sas.symget('sas_z')
    print(r['LOG'])
    sas.endsas()
    print('DEC: SAS value is: ' + str(z))
    return z


@task(log_stdout=True)
def list_sum(arr):
    return sum(arr)


with Flow(FLOW_NAME,
          storage=STORAGE,
          run_config=RUN_CONFIG,
          executor=EXECUTOR,) as flow:
    incs = inc.map(x=range(5))
    decs = dec.map(x=range(5))
    adds = add.map(x=incs, y=decs)
    total = list_sum(adds)
