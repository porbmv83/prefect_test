from prefect import task, Flow
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
#from dask_kubernetes import make_pod_spec
#from dask_kubernetes import KubeCluster
from prefect.storage import GitHub
import requests
import time



FLOW_NAME = "local_dask_compute_server_class"
STORAGE = GitHub(
    repo="porbmv83/prefect_test",
    path=f"{FLOW_NAME}.py",
    # access_token_secret="GITHUB_ACCESS_TOKEN",   required with private repositories
)

EXECUTOR = LocalDaskExecutor(
    scheduler="processes"
)

RUN_CONFIG = KubernetesRun(
    image="sasporbmvacr.azurecr.io/prefect-dask-spre:latest",
    env={"EXTRA_PIP_PACKAGES": "prefect[github]"},
    image_pull_secrets=["sasporbmvacr-image-pull-secret"],
    labels=["porbmv"],
)

@task(log_stdout=True)
def connectToComputeServer(server, numiterations):
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
    result = {'server':server, 'session_id':session_id, 'authheader':authheader}
    return [result]*numiterations

def runSASCode(code, con):
#    Since work is shared by all the code we have to write to unique datasets (expecting a string - datasetName:code to run)
    server = con.get("server")
    session_id = con.get("session_id")
    authheader = con.get("authheader")
    split = code.split(':')
    code = split[1]
    data = "{\"code\" : \"" + code + "\"}"
    url = server + '/compute/sessions/' + session_id + '/jobs'
    resp = requests.post(url=url, headers=authheader, data=data, verify=False)
    job_id = resp.json().get('id')

    # Get the job state (poll until it has completed)
    url = server + '/compute/sessions/' + session_id + '/jobs/' + job_id + '/state'
    state = 'running'
    while state == 'running':
        time.sleep(1)
        resp = requests.get(url=url, headers=authheader, verify=False)
        state = resp.content.decode("utf-8")
        print('The state is:' + state)


    # Read the result dataset value that is in work
    url = server + '/compute/sessions/' + session_id + '/jobs/' + job_id + "/data/WORK/" + split[0] + "/rows"


    resp = requests.get(url=url, headers=authheader, verify=False)
    val = resp.json().get('items')[0].get('cells')[0]
    print("SAS code:" + code + " return value: "+ str(val))
    return val

@task(log_stdout=True)
def inc(x,con):
    dsName = "INC" + str(x)

    code = "%let sas_x = " + str(x) + ";data " + dsName + ";sas_z = &sas_x+1;put 'result=' sas_z;run;"
    return runSASCode(dsName + ":" + code,con)



@task(log_stdout=True)
def dec(x,con):
    dsName = "DEC" + str(x)
    code= "%let sas_x = " + str(x) + ";data " + dsName + ";sas_z = &sas_x-1;put 'result=' sas_z;run;"
    return runSASCode(dsName + ":" + code,con)


@task(log_stdout=True)
def add(x, y, con):
    dsName = "ADD" + str(x)
    code= "%let sas_x = " + str(x) + ";%let sas_y = " + str(y) + ";data " + dsName + ";sas_z = &sas_x-&sas_y;run;"
    return runSASCode(dsName + ":" + code,con)


@task(log_stdout=True)
def list_sum(arr):
    totalsum = sum(arr)
    print("Final sum:" + str(totalsum))
    return totalsum

@task(log_stdout=True)
def disconnectFromComputeServer(total, con):
    server = con.get("server")
    session_id = con.get("session_id")
    authheader = con.get("authheader")
    # Delete the sas compute server session
    url = server + '/compute/sessions/' + session_id
    requests.delete(url=url, headers=authheader, verify=False)

with Flow(FLOW_NAME,
          storage=STORAGE,
          run_config=RUN_CONFIG,
          executor=EXECUTOR,) as flow:
    iterations = 100
    con = connectToComputeServer('https://d44242.rqs2porbmv-azure-nginx-a8329399.unx.sas.com', iterations)

    incs = inc.map(x=range(iterations), con=con)
    decs = dec.map(x=range(iterations), con=con)
    adds = add.map(x=incs, y=decs,con=con)
    total = list_sum(adds)
    disconnectFromComputeServer(total, con[0])
    print(total)
