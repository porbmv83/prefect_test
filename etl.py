from prefect import task, Flow, Parameter
from prefect.storage import GitHub
from prefect.run_configs import KubernetesRun

FLOW_NAME = "etl"
STORAGE = GitHub(
    repo="porbmv83/prefect_test",
    path=f"{FLOW_NAME}.py",
    #access_token_secret="GITHUB_ACCESS_TOKEN",   required with private repositories
)

@task
def extract():
    return [1, 2, 3]


@task
def transform_1(x1):
    return [i * 10 for i in x1]

@task
def transform_2(x2):
    return [i * 10 for i in x2]

@task
def load(y, z):
    print("Received y: {}".format(y))
    print("Received z: {}".format(z))


with Flow(FLOW_NAME, storage=STORAGE, run_config=KubernetesRun(labels=["porbmv"],),) as flow:
    e = extract()
    t1 = transform_1(e)
    t2 = transform_2(e)
    l = load(t1, t2)
