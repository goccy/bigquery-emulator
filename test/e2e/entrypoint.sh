#!/bin/sh
# Dispatch to a single client runner. The runner reads CASES_FILE, executes
# every query against the emulator at EMULATOR_HOST, and writes a per-case
# report to REPORT_FILE. It always exits 0 on a completed run (pass/fail is
# carried in the report); a non-zero exit means the runner itself crashed.
set -e

# Each client exports its own interpreter search paths. Setting
# PYTHONPATH / GEM_PATH / NODE_PATH globally in the Dockerfile leaked
# into the bq subprocess (which ships its own bundled deps) and broke
# bq whenever /deps/python's transitive deps drifted from those bundled
# with the Google Cloud SDK — see the comment in test/e2e/Dockerfile.
case "$1" in
  python) PYTHONPATH=/deps/python exec python3 /app/python/runner.py ;;
  ruby)   GEM_HOME=/deps/ruby GEM_PATH=/deps/ruby exec ruby /app/ruby/runner.rb ;;
  php)    exec php /app/php/runner.php ;;
  node)   NODE_PATH=/deps/node/node_modules exec node /app/node/runner.js ;;
  bq)     exec python3 /app/bq/runner.py ;;
  java)   exec java -jar /deps/java/runner.jar ;;
  *)
    echo "unknown client language: '$1' (want one of: python ruby php node bq java)" >&2
    exit 2
    ;;
esac
