from prefect import task, Flow
from prefect.executors import DaskExecutor
from prefect.run_configs import KubernetesRun
from dask_kubernetes import KubeCluster
from prefect.storage import GitHub

FLOW_NAME = "dask_spre"
STORAGE = GitHub(
    repo="porbmv83/prefect_test",
    path=f"{FLOW_NAME}.py",
    # access_token_secret="GITHUB_ACCESS_TOKEN",   required with private repositories
)


worker_spec = {
    "kind": "Pod",
    "metadata": {
        "labels": {
            "app": "prefect-spre"
        }
    },
    "spec": {
        "volumes": [
            {
                "name": "core",
                "persistentVolumeClaim": {
                    "claimName": "sas-risk-cirrus-core-pvc"
                }
            }
        ],
        "restartPolicy": "Never",
        "containers": [
            {
                "image": "sasporbmvacr.azurecr.io/prefect-dask-spre:latest",
                "imagePullPolicy": "IfNotPresent",
                "args": [
                    "dask-worker",
                    "$(DASK_SCHEDULER_ADDRESS)",
                    "--nthreads",
                    "2",
                    "--memory-limit",
                    "2GB",
                    "--no-dashboard",
                    "--death-timeout",
                    "60"
                ],
                "name": "dask-worker",
                "env": {
                    "resources": {
                        "limits": {
                            "cpu": "2",
                            "memory": "2G"
                        },
                        "requests": {
                            "cpu": "2",
                            "memory": "1G"
                        }
                    }
                },
                "volumeMounts": [
                    {
                        "name": "core",
                        "mountPath": "/core"
                    }
                ]
            }
        ],
        "imagePullSecrets": [
            {
                "name": "sasporbmvacr-image-pull-secret"
            }
        ],
        "nodeSelector": {
            "workload.sas.com/class": "compute"
        },
        "tolerations": [
            {
                "effect": "NoSchedule",
                "key": "workload.sas.com/class",
                "operator": "Equal",
                "value": "compute"
            }
        ]
    }
}


EXECUTOR = DaskExecutor(
    cluster_class=lambda: KubeCluster(worker_spec),
    adapt_kwargs={"minimum": 1, "maximum": 2},
)

RUN_CONFIG = KubernetesRun(
    env={
        "EXTRA_PIP_PACKAGES": "dask distributed dask-kubernetes"},
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
