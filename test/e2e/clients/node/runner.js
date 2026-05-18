'use strict';
// Node.js BigQuery client conformance runner.
//
// Reads the shared corpus (CASES_FILE), runs every query against the emulator
// at EMULATOR_HOST with the official @google-cloud/bigquery client, then
// normalizes and compares each result against the case's `expected` block.
//
// The comparison logic is intentionally Node-specific (the client's
// BigQueryTimestamp / BigQueryDate wrapper objects, integers as numbers) --
// verifying that per-language mapping is the point of the suite.

const fs = require('fs');
const {BigQuery} = require('@google-cloud/bigquery');

function pad(n) {
  return String(n).padStart(2, '0');
}

function formatTimestamp(d) {
  return `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())}` +
    `T${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(d.getUTCSeconds())}Z`;
}

function normalize(value) {
  if (value === null || value === undefined) {
    return null;
  }
  const t = typeof value;
  if (t === 'boolean' || t === 'number' || t === 'string') {
    return value;
  }
  if (t === 'bigint') {
    return Number(value);
  }
  if (Buffer.isBuffer(value)) {
    return value.toString('base64');
  }
  if (Array.isArray(value)) {
    return value.map(normalize);
  }
  if (t === 'object') {
    const name = value.constructor && value.constructor.name;
    switch (name) {
      case 'BigQueryTimestamp':
        return formatTimestamp(new Date(value.value));
      case 'BigQueryDate':
      case 'BigQueryDatetime':
      case 'BigQueryTime':
        return String(value.value);
      case 'BigQueryInt':
        return Number(value.value);
      case 'Big': // NUMERIC / BIGNUMERIC, via big.js
        return value.toString();
      default: {
        const out = {};
        for (const key of Object.keys(value)) {
          out[key] = normalize(value[key]);
        }
        return out;
      }
    }
  }
  return String(value);
}

function valuesEqual(actual, expected) {
  if (typeof actual === 'number' && typeof expected === 'number') {
    return Math.abs(actual - expected) <= 1e-9;
  }
  if (Array.isArray(actual) && Array.isArray(expected)) {
    return actual.length === expected.length &&
      actual.every((v, i) => valuesEqual(v, expected[i]));
  }
  if (actual && expected && typeof actual === 'object' && typeof expected === 'object') {
    const ka = Object.keys(actual);
    const ke = Object.keys(expected);
    return ka.length === ke.length && ke.every((k) => valuesEqual(actual[k], expected[k]));
  }
  return actual === expected;
}

async function runCase(bigquery, testCase) {
  const options = {query: testCase.sql};
  if (testCase.params && testCase.params.length) {
    options.params = {};
    for (const p of testCase.params) {
      options.params[p.name] = p.value;
    }
  }
  const [rows] = await bigquery.query(options);
  const actual = rows.map(normalize);
  if (valuesEqual(actual, testCase.expected)) {
    return {name: testCase.name, status: 'pass', detail: ''};
  }
  return {
    name: testCase.name,
    status: 'fail',
    detail: `expected ${JSON.stringify(testCase.expected)} got ${JSON.stringify(actual)}`,
  };
}

async function main() {
  const host = process.env.EMULATOR_HOST;
  const projectId = process.env.PROJECT_ID || 'test';
  const cases = JSON.parse(fs.readFileSync(process.env.CASES_FILE, 'utf8')).cases;
  const reportFile = process.env.REPORT_FILE;

  let results = [];
  try {
    // A custom apiEndpoint marks this as a non-default endpoint, so the client
    // skips authentication -- exactly what is wanted against the emulator.
    const bigquery = new BigQuery({projectId, apiEndpoint: host});
    for (const testCase of cases) {
      try {
        results.push(await runCase(bigquery, testCase));
      } catch (err) {
        results.push({name: testCase.name, status: 'error', detail: `${err.name}: ${err.message}`});
      }
    }
  } catch (err) {
    const detail = `${err.name}: ${err.message}`;
    results = cases.map((c) => ({name: c.name, status: 'error', detail}));
  }

  fs.writeFileSync(reportFile, JSON.stringify({language: 'node', cases: results}));
  const failed = results.filter((r) => r.status !== 'pass').length;
  console.log(`node: ${results.length} case(s), ${failed} not passing`);
  process.exit(0);
}

main();
