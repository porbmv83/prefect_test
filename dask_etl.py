from prefect import task, Flow, Parameter
import datetime
import random
from prefect.storage import GitHub
from prefect.run_configs import KubernetesRun
from prefect.executors import DaskExecutor
from dask_cloudprovider.aws import FargateCluster

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

with Flow(FLOW_NAME, storage=STORAGE, run_config=KubernetesRun(labels=["porbmv"],), executor = DaskExecutor(
    cluster_class="dask_cloudprovider.aws.FargateCluster",
    cluster_kwargs={"n_workers": 4, "image": "my-prefect-image"},)) as flow:
    incs = inc.map(x=range(100))
    decs = dec.map(x=range(100))
    adds = add.map(x=incs, y=decs)
    total = list_sum(adds)
