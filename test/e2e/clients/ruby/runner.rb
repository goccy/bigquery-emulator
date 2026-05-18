# Ruby BigQuery client conformance runner.
#
# Reads the shared corpus (CASES_FILE), runs every query against the emulator
# at EMULATOR_HOST with the official `google-cloud-bigquery` gem, then
# normalizes and compares each result against the case's `expected` block.
#
# The comparison logic is intentionally Ruby-specific (Time/Date/BigDecimal
# mapping) -- verifying that per-language mapping is the point of the suite.

require "json"
require "date"
require "bigdecimal"
require "stringio"
require "google/cloud/bigquery"
require "googleauth"

# The emulator performs no token validation, so we hand the gem a credentials
# object whose signing client is a no-op. This avoids any Application Default
# Credentials lookup, which would fail inside the test container.
class NoAuthCredentials < Google::Auth::Credentials
  def initialize; end

  def client
    @client ||= begin
      c = Object.new
      def c.apply!(hash, _opts = {}); hash; end
      def c.apply(hash, _opts = {}); hash.dup; end
      def c.updater_proc; ->(hash, _opts = {}) { hash }; end
      def c.expires_within?(_sec); false; end
      def c.expired?; false; end
      def c.fetch_access_token!(_options = {}); {}; end
      c
    end
  end
end

def normalize(value)
  case value
  when nil then nil
  when true, false then value
  when Integer, Float then value
  when BigDecimal then value.to_s("F")
  when Time then value.utc.strftime("%Y-%m-%dT%H:%M:%SZ")
  when DateTime then value.new_offset(0).strftime("%Y-%m-%dT%H:%M:%SZ")
  when Date then value.strftime("%Y-%m-%d")
  when StringIO then [value.string].pack("m0")
  when Array then value.map { |v| normalize(v) }
  when Hash then value.each_with_object({}) { |(k, v), h| h[k.to_s] = normalize(v) }
  else value.to_s
  end
end

def numbers_equal(a, b)
  (a.to_f - b.to_f).abs <= 1e-9
end

def values_equal(actual, expected)
  if [true, false].include?(expected) || [true, false].include?(actual)
    actual == expected
  elsif expected.is_a?(Numeric) && actual.is_a?(Numeric)
    numbers_equal(actual, expected)
  elsif expected.is_a?(Array) && actual.is_a?(Array)
    actual.length == expected.length &&
      actual.zip(expected).all? { |x, y| values_equal(x, y) }
  elsif expected.is_a?(Hash) && actual.is_a?(Hash)
    actual.keys.sort == expected.keys.sort &&
      expected.all? { |k, v| values_equal(actual[k], v) }
  else
    actual == expected
  end
end

def run_case(bigquery, kase)
  params = {}
  (kase["params"] || []).each { |p| params[p["name"].to_sym] = p["value"] }
  data = if params.empty?
           bigquery.query kase["sql"]
         else
           bigquery.query kase["sql"], params: params
         end
  rows = data.map { |row| normalize(row.to_h) }
  expected = kase["expected"]
  if values_equal(rows, expected)
    { "name" => kase["name"], "status" => "pass", "detail" => "" }
  else
    {
      "name" => kase["name"],
      "status" => "fail",
      "detail" => "expected #{expected.to_json} got #{rows.to_json}",
    }
  end
end

# The gem treats `endpoint` as the REST root URL and concatenates the API
# base path onto it, so it must carry a trailing slash.
host = ENV.fetch("EMULATOR_HOST")
host += "/" unless host.end_with?("/")
project = ENV.fetch("PROJECT_ID", "test")
cases = JSON.parse(File.read(ENV.fetch("CASES_FILE")))["cases"]
report_file = ENV.fetch("REPORT_FILE")

results = []
begin
  bigquery = Google::Cloud::Bigquery.new(
    project_id: project,
    endpoint: host,
    credentials: NoAuthCredentials.new,
  )
  cases.each do |kase|
    begin
      results << run_case(bigquery, kase)
    rescue StandardError => e
      results << {
        "name" => kase["name"],
        "status" => "error",
        "detail" => "#{e.class}: #{e.message}",
      }
    end
  end
rescue StandardError => e
  detail = "#{e.class}: #{e.message}"
  results = cases.map do |kase|
    { "name" => kase["name"], "status" => "error", "detail" => detail }
  end
end

File.write(report_file, JSON.generate("language" => "ruby", "cases" => results))
failed = results.count { |r| r["status"] != "pass" }
puts "ruby: #{results.length} case(s), #{failed} not passing"
exit 0
