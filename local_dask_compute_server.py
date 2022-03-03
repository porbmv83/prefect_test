from prefect import task, Flow
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
#from dask_kubernetes import make_pod_spec
#from dask_kubernetes import KubeCluster
from prefect.storage import GitHub
import requests
import time



FLOW_NAME = "local_dask_compute_server"
STORAGE = GitHub(
    repo="porbmv83/prefect_test",
    path=f"{FLOW_NAME}.py",
    # access_token_secret="GITHUB_ACCESS_TOKEN",   required with private repositories
)

EXECUTOR = LocalDaskExecutor(
    scheduler="threads",
    num_workers=8,
)

RUN_CONFIG = KubernetesRun(
    image="sasporbmvacr.azurecr.io/prefect-dask-spre:latest",
    env={"EXTRA_PIP_PACKAGES": "prefect[github]"},
    image_pull_secrets=["sasporbmvacr-image-pull-secret"],
    labels=["porbmv"],
)

@task(log_stdout=True, nout=2)
def connectToComputeServer():
    # Log on to sas
    url = server + '/SASLogon/oauth/token/'
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    data = 'grant_type=password&username=sasadm&password=Go4thsas'
    auth = requests.auth.HTTPBasicAuth('sas.ec', '')
    resp = requests.post(url=url, headers=headers, data=data, auth=auth, verify=False)

    # get the access token
    access_token = resp.json().get('access_token')
    print("Obtained access token:" + access_token)
    # Create a compute server session
    url = server + '/compute/contexts/de6c8b23-59ac-4993-9a5d-376b0d662045/sessions'
    authheader = {'Authorization': 'Bearer ' + access_token}
    data = '{"contextId" : "c5b69c5d-4596-41bc-a07f-f6ba0057f1b5", "command" : "/opt/sas/viya/home/bin/compsrv_start.sh"}'
    resp = requests.post(url=url, headers=authheader, data=data, verify=False)
    session_id = resp.json().get('id')
    print("Got a compute server session:" + session_id)
    return session_id, authheader

@task(log_stdout=True)
def runSASCode(code, server, session_id, authheader):
    print(code)
    print(server)
    print(session_id)
    data = "{\"code\" : \"" + code + "\"}"
    url = server + '/compute/sessions/' + session_id + '/jobs'
    print(url)
    print(data)
    resp = requests.post(url=url, headers=authheader, data=data, verify=False)
    print(resp.json())
    job_id = resp.json().get('id')

    # Get the job state (poll until it has completed)
    url = server + '/compute/sessions/' + session_id + '/jobs/' + job_id + '/state'
    state = 'running'
    while state == 'running':
        time.sleep(1)
        resp = requests.get(url=url, headers=authheader, verify=False)
        state = resp.content.decode("utf-8")
        print('The state is:' + state)


    # Get the results for the job
    url = server + '/compute/sessions/' + session_id + '/jobs/' + job_id + "/data/WORK/RESULT/rows"

    resp = requests.get(url=url, headers=authheader, verify=False)
    return resp.json().get('items')[0].get('cells')[0]


@task(log_stdout=True)
def inc(x):
    code = "%let sas_x = " + str(x) + ";data result;sas_z = &sas_x+1;put 'result=' sas_z;run;"
    return code



@task(log_stdout=True)
def dec(x):
    code= "%let sas_x = " + str(x) + ";data result;sas_z = &sas_x-1;put 'result=' sas_z;run;"
    return code


@task(log_stdout=True)
def add(x, y):
    code= "%let sas_x = " + str(x) +";%let sas_y = " + str(y) + ";data result;sas_z = &sas_x-&sas_y;run;"
    return code


@task(log_stdout=True)
def list_sum(arr):
    return sum(arr)


with Flow(FLOW_NAME,
          storage=STORAGE,
          run_config=RUN_CONFIG,
          executor=EXECUTOR,) as flow:
    server = 'https://d44242.rqs2porbmv-azure-nginx-a8329399.unx.sas.com'
    authheader = ''
    session_id, authheader = connectToComputeServer()

    iterations = 10
    servlist = [server] * iterations
    sessionlist = [session_id] * iterations
    authlist = [authheader] * iterations

    incode = inc.map(x=range(iterations))
    incs = runSASCode.map(code=incode, server=servlist, session_id=sessionlist, authheader=authlist)

    deccode = dec.map(x=range(iterations))
    decs = runSASCode.map(code=deccode, server=servlist, session_id=sessionlist, authheader=authlist)

    addcode = add.map(x=incs, y=decs)
    adds = runSASCode.map(code=addcode, server=servlist, session_id=sessionlist, authheader=authlist)
    total = list_sum(adds)

    print(total)
