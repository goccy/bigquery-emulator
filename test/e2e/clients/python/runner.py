"""Python BigQuery client conformance runner.

Reads the shared corpus (CASES_FILE), runs every query against the emulator
at EMULATOR_HOST with the official ``google-cloud-bigquery`` client, then
normalizes and compares each result against the case's ``expected`` block.

The comparison logic here is intentionally Python-specific: every client
library returns BigQuery values as different native types, and exercising
that per-language mapping is the whole point of the conformance suite.
"""

import base64
import datetime
import decimal
import json
import os
import sys
import traceback

import requests
from google.api_core import exceptions as api_exceptions
from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery

# A wedged or unreachable emulator surfaces as one of these; the rest of the
# run is abandoned rather than retried for minutes per remaining case.
TRANSPORT_ERRORS = (
    api_exceptions.RetryError,
    api_exceptions.ServiceUnavailable,
    api_exceptions.DeadlineExceeded,
    requests.exceptions.RequestException,
    ConnectionError,
    TimeoutError,
)

# Per-request and per-job-completion timeout, in seconds.
REQUEST_TIMEOUT = 20


def normalize(value):
    """Map a value returned by the Python client to the canonical form."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return value
    if isinstance(value, decimal.Decimal):
        return str(value)
    if isinstance(value, (bytes, bytearray)):
        return base64.b64encode(bytes(value)).decode("ascii")
    if isinstance(value, datetime.datetime):
        utc = value.astimezone(datetime.timezone.utc) if value.tzinfo else value
        return utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    if isinstance(value, datetime.date):
        return value.isoformat()
    if isinstance(value, datetime.time):
        return value.strftime("%H:%M:%S")
    if isinstance(value, (list, tuple)):
        return [normalize(v) for v in value]
    if isinstance(value, dict):
        return {k: normalize(v) for k, v in value.items()}
    if hasattr(value, "items"):  # bigquery.Row exposes items()
        return {k: normalize(v) for k, v in value.items()}
    return str(value)


def numbers_equal(a, b):
    return abs(float(a) - float(b)) <= 1e-9


def values_equal(actual, expected):
    if isinstance(expected, bool) or isinstance(actual, bool):
        return actual == expected
    if isinstance(expected, (int, float)) and isinstance(actual, (int, float)):
        return numbers_equal(actual, expected)
    if isinstance(expected, list) and isinstance(actual, list):
        return len(actual) == len(expected) and all(
            values_equal(x, y) for x, y in zip(actual, expected)
        )
    if isinstance(expected, dict) and isinstance(actual, dict):
        return set(actual) == set(expected) and all(
            values_equal(actual[k], expected[k]) for k in expected
        )
    return actual == expected


def make_param(spec):
    return bigquery.ScalarQueryParameter(spec["name"], spec["type"], spec["value"])


def run_case(client, case):
    params = [make_param(p) for p in case.get("params", [])]
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    # Bound both the per-request timeout and the overall retry window so a
    # wedged emulator fails the case quickly instead of retrying for minutes.
    retry = bigquery.DEFAULT_RETRY.with_timeout(REQUEST_TIMEOUT)
    job = client.query(
        case["sql"], job_config=job_config, timeout=REQUEST_TIMEOUT, retry=retry
    )
    rows = [normalize(dict(row)) for row in job.result(timeout=REQUEST_TIMEOUT, retry=retry)]
    expected = case["expected"]
    if values_equal(rows, expected):
        return {"name": case["name"], "status": "pass", "detail": ""}
    return {
        "name": case["name"],
        "status": "fail",
        "detail": "expected {} got {}".format(
            json.dumps(expected), json.dumps(rows)
        ),
    }


def main():
    host = os.environ["EMULATOR_HOST"]
    project = os.environ.get("PROJECT_ID", "test")
    cases_file = os.environ["CASES_FILE"]
    report_file = os.environ["REPORT_FILE"]

    with open(cases_file) as fh:
        cases = json.load(fh)["cases"]

    results = []
    try:
        client = bigquery.Client(
            project,
            client_options=ClientOptions(api_endpoint=host),
            credentials=AnonymousCredentials(),
        )
        aborted = None
        for case in cases:
            if aborted is not None:
                results.append({"name": case["name"], "status": "error", "detail": aborted})
                continue
            try:
                results.append(run_case(client, case))
            except TRANSPORT_ERRORS as exc:
                # The emulator is unreachable or unresponsive; abandon the rest
                # of the run instead of retrying every remaining case.
                aborted = "aborted: emulator unreachable/unresponsive: {}".format(exc)
                results.append({"name": case["name"], "status": "error", "detail": aborted})
            except Exception:  # noqa: BLE001 - one bad case must not abort the run
                results.append(
                    {
                        "name": case["name"],
                        "status": "error",
                        "detail": traceback.format_exc(limit=3),
                    }
                )
    except Exception:  # noqa: BLE001 - client setup failed; mark every case
        detail = traceback.format_exc(limit=3)
        results = [
            {"name": c["name"], "status": "error", "detail": detail} for c in cases
        ]

    report = {"language": "python", "cases": results}
    with open(report_file, "w") as fh:
        json.dump(report, fh)

    failed = sum(1 for r in results if r["status"] != "pass")
    print("python: {} case(s), {} not passing".format(len(results), failed))
    sys.exit(0)


if __name__ == "__main__":
    main()
