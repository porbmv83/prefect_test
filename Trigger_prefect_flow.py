# You can control how the API versions your flows by providing a version_group_id
# whenever you register a flow (exposed via the version_group_id keyword argument
# in flow.register). Flows which provide the same version_group_id will be considered
# versions of each other. By default, flows with the same name in the same Project will
# be given the same version_group_id and are considered "versions" of each other.
# Anytime you register a new version of a flow, Prefect API will automatically "archive"
# the old version in place of the newly registered flow. Archiving means that the old version's
# schedule is set to "Paused" and no new flow runs can be created.

import requests

create_mutation = """
mutation($input: createFlowRunInput!){
    createFlowRun(input: $input){
        flow_run{
            id
        }
    }
}
"""

inputs = dict(
    versionGroupId="339c86be-5c1c-48f0-b8d3-fe57654afe22", parameters=dict(x=6)
)
response = requests.post(
    url="https://api.prefect.io",
    json=dict(query=create_mutation, variables=dict(input=inputs)),
    headers=dict(authorization=f"Bearer {API_KEY}"),
)
print(response.status_code)
print(response.text)
