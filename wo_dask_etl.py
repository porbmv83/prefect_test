from prefect import task, Flow, Parameter
import datetime
import random
from prefect.storage import GitHub
from prefect.run_configs import KubernetesRun
from prefect.environments import KubeCluster
from prefect.executors import DaskExecutor

FLOW_NAME = "wo_dask_etl"
STORAGE = GitHub(
    repo="porbmv83/prefect_test",
    path=f"{FLOW_NAME}.py",
    #access_token_secret="GITHUB_ACCESS_TOKEN",   required with private repositories
)

EXECUTOR = DaskExecutor(
    cluster_class="dask_kubernetes.KubeCluster",
    cluster_kwargs={
        "image": "prefecthq/prefect:latest",
        "n_workers": 2,
    },
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

with Flow(run_config=KubernetesRun(env={"EXTRA_PIP_PACKAGES": "prefect dask distributed"}, labels=["porbmv"],),  
        executor = EXECUTOR,) as flow:
    incs = inc.map(x=range(100))
    decs = dec.map(x=range(100))
    adds = add.map(x=incs, y=decs)
    total = list_sum(adds)
