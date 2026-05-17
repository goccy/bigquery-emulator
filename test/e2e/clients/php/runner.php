<?php
/**
 * PHP BigQuery client conformance runner.
 *
 * Reads the shared corpus (CASES_FILE), runs every query against the emulator
 * at EMULATOR_HOST with the official google/cloud-bigquery client, then
 * normalizes and compares each result against the case's `expected` block.
 *
 * The comparison logic is intentionally PHP-specific (Date/Timestamp/Bytes
 * wrapper objects) -- verifying that per-language mapping is the point.
 */

require '/deps/php/vendor/autoload.php';

use Google\Cloud\Core\AnonymousCredentials;
use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\BigQuery\Bytes;
use Google\Cloud\BigQuery\Date;
use Google\Cloud\BigQuery\Time;
use Google\Cloud\BigQuery\Timestamp;

function normalize($v)
{
    if ($v === null) {
        return null;
    }
    if (is_bool($v) || is_int($v) || is_float($v) || is_string($v)) {
        return $v;
    }
    if ($v instanceof Date) {
        return $v->formatAsString();
    }
    if ($v instanceof Time) {
        return $v->formatAsString();
    }
    if ($v instanceof Timestamp) {
        return $v->get()->setTimezone(new DateTimeZone('UTC'))->format('Y-m-d\TH:i:s\Z');
    }
    if ($v instanceof Bytes) {
        return base64_encode((string) $v);
    }
    if ($v instanceof DateTimeInterface) {
        return $v->setTimezone(new DateTimeZone('UTC'))->format('Y-m-d\TH:i:s\Z');
    }
    if (is_array($v)) {
        if (array_is_list($v)) {
            return array_map('normalize', $v);
        }
        $out = [];
        foreach ($v as $k => $vv) {
            $out[$k] = normalize($vv);
        }
        return $out;
    }
    return (string) $v;
}

function numbers_equal($a, $b)
{
    return abs((float) $a - (float) $b) <= 1e-9;
}

function values_equal($actual, $expected)
{
    if (is_bool($expected) || is_bool($actual)) {
        return $actual === $expected;
    }
    if (is_numeric($expected) && is_numeric($actual) && !is_string($expected) && !is_string($actual)) {
        return numbers_equal($actual, $expected);
    }
    if (is_array($expected) && is_array($actual)) {
        if (count($expected) !== count($actual)) {
            return false;
        }
        foreach ($expected as $k => $v) {
            if (!array_key_exists($k, $actual) || !values_equal($actual[$k], $v)) {
                return false;
            }
        }
        return true;
    }
    return $actual === $expected;
}

function run_case(BigQueryClient $bq, array $case): array
{
    $config = $bq->query($case['sql']);
    $params = [];
    foreach ($case['params'] ?? [] as $p) {
        $params[$p['name']] = $p['value'];
    }
    if ($params) {
        $config = $config->parameters($params);
    }
    $results = $bq->runQuery($config);

    $rows = [];
    foreach ($results->rows() as $row) {
        $rows[] = normalize($row);
    }
    $expected = $case['expected'];
    if (values_equal($rows, $expected)) {
        return ['name' => $case['name'], 'status' => 'pass', 'detail' => ''];
    }
    return [
        'name' => $case['name'],
        'status' => 'fail',
        'detail' => 'expected ' . json_encode($expected) . ' got ' . json_encode($rows),
    ];
}

$host = getenv('EMULATOR_HOST');
$project = getenv('PROJECT_ID') ?: 'test';
$cases = json_decode(file_get_contents(getenv('CASES_FILE')), true)['cases'];
$reportFile = getenv('REPORT_FILE');

$results = [];
try {
    $bq = new BigQueryClient([
        'projectId' => $project,
        'apiEndpoint' => $host,
        'credentialsFetcher' => new AnonymousCredentials(),
    ]);
    foreach ($cases as $case) {
        try {
            $results[] = run_case($bq, $case);
        } catch (Throwable $e) {
            $results[] = [
                'name' => $case['name'],
                'status' => 'error',
                'detail' => get_class($e) . ': ' . $e->getMessage(),
            ];
        }
    }
} catch (Throwable $e) {
    $detail = get_class($e) . ': ' . $e->getMessage();
    $results = array_map(
        fn ($case) => ['name' => $case['name'], 'status' => 'error', 'detail' => $detail],
        $cases
    );
}

file_put_contents($reportFile, json_encode(['language' => 'php', 'cases' => $results]));
$failed = count(array_filter($results, fn ($r) => $r['status'] !== 'pass'));
echo 'php: ' . count($results) . " case(s), $failed not passing\n";
exit(0);
