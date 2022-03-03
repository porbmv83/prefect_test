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

@task(log_stdout=True)
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
def runSASCode(code, server, session_id, autheader):
    data = "{\"code\" : \"" + code + "\""
    url = server + '/compute/sessions/' + session_id + '/jobs'
    resp = requests.post(url=url, headers=authheader, data=data, verify=False)
    job_id = resp.json().get('id')

    # Get the job state (poll until it has completed)
    url = server + '/compute/sessions/' + session_id + '/jobs/' + job_id + '/state'
    state = 'running'
    while state == 'running':
        resp = requests.get(url=url, headers=authheader, verify=False)
        state = resp.content.decode("utf-8")
        print('The state is:' + state)
        time.sleep(10)

    # Get the log for the job
    url = server + '/compute/sessions/' + session_id + '/log/' + job_id

    authheader['Accept'] = 'text/plain'
    resp = requests.get(url=url, headers=authheader, verify=False)
    print(resp.content.decode("utf-8"))


@task(log_stdout=True)
def inc(x, session_id):

    print("Python value for inc: "+str(x))

    code = ("%let sas_x = " + str(x) + ";" +
            """"
		    data _null_;
			    sas_z = &sas_x+1;
			    call symput('sas_z', sas_z);
		    run; 
	        """)
    return code



# @task(log_stdout=True)
# def dec(x):
#     print("Python value for dec: "+str(x))
#     sas = saspy.SASsession()
#     sas.symput('sas_x', x)
#     r = sas.submit("""
# 		%put DEC: Python value is: &sas_x;
# 		data _null_;
# 			sas_z = &sas_x-1;
# 			call symput('sas_z', sas_z);
# 		run;
# 	""")
#     z = sas.symget('sas_z')
#     print(r['LOG'])
#     sas.endsas()
#     print('DEC: SAS value is: ' + str(z))
#     return z
#
#
# @task(log_stdout=True)
# def add(x, y):
#     print("Python value for add: "+str(x)+str(y))
#     sas = saspy.SASsession()
#     sas.symput('sas_x', x)
#     sas.symput('sas_y', y)
#     r = sas.submit("""
# 		%put ADD: Python value is: &sas_x and &sas_y;
# 		data _null_;
# 			sas_z = &sas_x-&sas_y;
# 			call symput('sas_z', sas_z);
# 		run;
# 	""")
#     z = sas.symget('sas_z')
#     print(r['LOG'])
#     sas.endsas()
#     print('DEC: SAS value is: ' + str(z))
#     return z


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
    code = inc(x=1, session_id=session_id)
    runSASCode(code, server, session_id, authheader)

