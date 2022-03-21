# Input is the flow id wich can ge retrieved from the createFlowRun query resonse

import requests

flow_run_info = """
query($input: uuid!) {
  flow_run_by_pk(id: $input) {
    id
    name
  }
}
"""

response = requests.post(
    url="https://api.prefect.io",
    json=dict(query=flow_run_info, variables=dict(
        input="6764bf4d-7de0-4682-a5ae-8b9b2c744ced")),
    headers=dict(authorization=f"Bearer {API_KEY}"),
)
print(response.status_code)
print(response.text)
