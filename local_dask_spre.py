from prefect import task, Flow
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
#from dask_kubernetes import make_pod_spec
#from dask_kubernetes import KubeCluster
from prefect.storage import GitHub
import saspy

FLOW_NAME = "local_dask_spre"
STORAGE = GitHub(
    repo="porbmv83/prefect_test",
    path=f"{FLOW_NAME}.py",
    # access_token_secret="GITHUB_ACCESS_TOKEN",   required with private repositories
)

EXECUTOR = LocalDaskExecutor(
    scheduler="processes",
)

RUN_CONFIG = KubernetesRun(
    image="sasporbmvacr.azurecr.io/prefect-dask-spre:latest",
    env={"EXTRA_PIP_PACKAGES": "prefect[github]"},
    image_pull_secrets=["sasporbmvacr-image-pull-secret"],
    labels=["porbmv"],
)


@task(log_stdout=True)
def inc(x):
    print("Python value for inc: "+str(x))
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
    print("Python value for dec: "+str(x))
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
    print("Python value for add: "+str(x)+str(y))
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
    incs = inc.map(x=range(100))
    decs = dec.map(x=range(100))
    adds = add.map(x=incs, y=decs)
    total = list_sum(adds)
