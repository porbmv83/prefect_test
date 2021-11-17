rom prefect import task, Flow, Parameter
import datetime
import random
from prefect.storage import GitHub
from prefect.run_configs import KubernetesRun
from prefect.executors import DaskExecutor

FLOW_NAME = "dask_etl"
STORAGE = GitHub(
    repo="porbmv83/prefect_test",
    path=f"{FLOW_NAME}.py",
    #access_token_secret="GITHUB_ACCESS_TOKEN",   required with private repositories
)

@task
def inc(x):
    return x + 1


@task
def dec(x):
    return x - 1

@task   
def add(x, y):
    return x + y

@task
def list_sum(arr):
    return sum(arr)

with Flow(FLOW_NAME, 
    storage=STORAGE, 
    run_config=KubernetesRun(env={"EXTRA_PIP_PACKAGES": "prefect dask distributed"}, labels=["porbmv"],), 
    executor = DaskExecutor(address="tcp://20.85.133.123:8786")
    ) as flow:
    incs = inc.map(x=range(100))
    decs = dec.map(x=range(100))
    adds = add.map(x=incs, y=decs)
    total = list_sum(adds)
