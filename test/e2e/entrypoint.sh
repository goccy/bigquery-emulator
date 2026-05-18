#!/bin/sh
# Dispatch to a single client runner. The runner reads CASES_FILE, executes
# every query against the emulator at EMULATOR_HOST, and writes a per-case
# report to REPORT_FILE. It always exits 0 on a completed run (pass/fail is
# carried in the report); a non-zero exit means the runner itself crashed.
set -e

case "$1" in
  python) exec python3 /app/python/runner.py ;;
  ruby)   exec ruby /app/ruby/runner.rb ;;
  php)    exec php /app/php/runner.php ;;
  node)   exec node /app/node/runner.js ;;
  bq)     exec python3 /app/bq/runner.py ;;
  java)   exec java -jar /deps/java/runner.jar ;;
  *)
    echo "unknown client language: '$1' (want one of: python ruby php node bq java)" >&2
    exit 2
    ;;
esac
