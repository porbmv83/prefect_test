from prefect import task, Flow, Parameter
from prefect.storage import GitHub
from prefect.run_configs import KubernetesRun

FLOW_NAME = "etl"
STORAGE = GitHub(
    repo="PrefectHQ/prefect",
    path=f"prefect/examples/old/{FLOW_NAME}.py",
    #access_token_secret="GITHUB_ACCESS_TOKEN",   required with private repositories
)

@task
def extract():
    return [1, 2, 3]


@task
def transform(x):
    return [i * 10 for i in x]


@task
def load(y):
    print("Received y: {}".format(y))


with Flow(FLOW_NAME, storage=STORAGE, run_config=KubernetesRun(labels=["porbmv"],),) as flow:
    e = extract()
    t = transform(e)
    l = load(t)

