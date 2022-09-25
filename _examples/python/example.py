from typing import List, Iterator, Dict, Any

from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery, exceptions

def main():
    client = bigquery.Client(
        "test",
        client_options=ClientOptions(api_endpoint="http://bigquery:9050"),
        credentials=AnonymousCredentials(),
    )
    job = client.query(
        query="SELECT * FROM dataset1.table_a",
        job_config=bigquery.QueryJobConfig(),
    )
    print(list(job.result()))

main()
