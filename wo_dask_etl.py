from prefect import task, Flow
from prefect.storage import GitHub
from prefect.run_configs import KubernetesRun
from prefect.executors import DaskExecutor
from dask_kubernetes import make_pod_spec

FLOW_NAME = "wo_dask_etl"
STORAGE = GitHub(
    repo="porbmv83/prefect_test",
    path=f"{FLOW_NAME}.py",
    # access_token_secret="GITHUB_ACCESS_TOKEN",   required with private repositories
)

POD_SPEC = make_pod_spec(
    image="prefecthq/prefect:0.15.9",
    memory_limit="2G",
    memory_request="2G",
    cpu_limit=1,
    cpu_request=1,
    env={"EXTRA_PIP_PACKAGES": "distributed",
         "TZ": "Europe/Amsterdam"},)

EXECUTOR = DaskExecutor(
    cluster_class="dask_kubernetes.KubeCluster",
    cluster_kwargs={"pod_template": POD_SPEC,
                    "name": "my-dask-pod"
                    },
    adapt_kwargs={"minimum": 1,
                  "maximum": 2},
)

RUN_CONFIG = KubernetesRun(
    env={
        "EXTRA_PIP_PACKAGES": "dask-kubernetes"},
    labels=["porbmv"],
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
          run_config=RUN_CONFIG,
          executor=EXECUTOR,) as flow:
    incs = inc.map(x=range(100))
    decs = dec.map(x=range(100))
    adds = add.map(x=incs, y=decs)
    total = list_sum(adds)
