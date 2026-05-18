"""bq command-line conformance runner.

Drives the official `bq` CLI (bundled with the Google Cloud SDK) against the
emulator and checks its rendered output against the shared corpus.

Unlike the library clients, `bq` is a command-line tool: it renders every
value as text and erases the JSON value types. The comparison here is
therefore text-based -- both the bq output and the expected values are reduced
to a canonical textual form before they are compared. Exercising that
CLI-specific rendering is the point of including bq in the matrix.
"""

import json
import os
import re
import subprocess
import sys
import traceback

# bq renders TIMESTAMP as "YYYY-MM-DD HH:MM:SS"; the corpus uses ISO 8601.
_TIMESTAMP_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})(?:\.\d+)?(?:[ ]?(?:UTC|Z|\+00(?::?00)?))?$")


def canonicalize(value):
    """Reduce a value (from bq output or from the corpus) to canonical text."""
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return str(int(value)) if value.is_integer() else repr(value)
    if isinstance(value, list):
        return [canonicalize(v) for v in value]
    if isinstance(value, dict):
        return {k: canonicalize(v) for k, v in value.items()}
    text = str(value)
    m = _TIMESTAMP_RE.match(text)
    if m:
        return "{}T{}Z".format(m.group(1), m.group(2))
    return text


def run_case(host, project, case):
    cmd = [
        "bq",
        "--api=" + host,
        "--project_id=" + project,
        "--format=prettyjson",
        "--headless",
        "--quiet",
        "query",
        "--use_legacy_sql=false",
    ]
    for p in case.get("params", []):
        cmd.append("--parameter={}:{}:{}".format(p["name"], p["type"], p["value"]))
    cmd.append(case["sql"])

    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=90)
    if proc.returncode != 0:
        detail = (proc.stderr.strip() or proc.stdout.strip() or "bq exited non-zero")
        return {"name": case["name"], "status": "error", "detail": detail}

    # bq prints the JSON array of rows; ignore anything before the first '['.
    out = proc.stdout
    start = out.find("[")
    rows = json.loads(out[start:]) if start >= 0 else []

    actual = canonicalize(rows)
    expected = canonicalize(case["expected"])
    if actual == expected:
        return {"name": case["name"], "status": "pass", "detail": ""}
    return {
        "name": case["name"],
        "status": "fail",
        "detail": "expected {} got {}".format(json.dumps(expected), json.dumps(actual)),
    }


def main():
    host = os.environ["EMULATOR_HOST"]
    project = os.environ.get("PROJECT_ID", "test")
    with open(os.environ["CASES_FILE"]) as fh:
        cases = json.load(fh)["cases"]
    report_file = os.environ["REPORT_FILE"]

    results = []
    for case in cases:
        try:
            results.append(run_case(host, project, case))
        except Exception:  # noqa: BLE001 - one bad case must not abort the run
            results.append(
                {
                    "name": case["name"],
                    "status": "error",
                    "detail": traceback.format_exc(limit=3),
                }
            )

    with open(report_file, "w") as fh:
        json.dump({"language": "bq", "cases": results}, fh)

    failed = sum(1 for r in results if r["status"] != "pass")
    print("bq: {} case(s), {} not passing".format(len(results), failed))
    sys.exit(0)


if __name__ == "__main__":
    main()
