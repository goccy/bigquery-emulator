package emulator.e2e;

import com.google.cloud.NoCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryParameterValue;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Java BigQuery client conformance runner.
 *
 * <p>Reads the shared corpus (CASES_FILE), runs every query against the
 * emulator at EMULATOR_HOST with the official google-cloud-bigquery client,
 * then normalizes and compares each result against the case's {@code expected}
 * block. The comparison logic is intentionally Java-specific (FieldValue /
 * schema-driven decoding) -- verifying that mapping is the point of the suite.
 */
public final class Runner {

  private static final DateTimeFormatter TS_FMT =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").withZone(ZoneOffset.UTC);

  public static void main(String[] args) throws Exception {
    String host = System.getenv("EMULATOR_HOST");
    String project = envOr("PROJECT_ID", "test");
    String casesFile = System.getenv("CASES_FILE");
    String reportFile = System.getenv("REPORT_FILE");

    JsonArray cases =
        JsonParser.parseString(new String(Files.readAllBytes(Paths.get(casesFile)), StandardCharsets.UTF_8))
            .getAsJsonObject()
            .getAsJsonArray("cases");

    JsonArray results = new JsonArray();
    try {
      BigQuery bq =
          BigQueryOptions.newBuilder()
              .setProjectId(project)
              .setHost(host)
              .setLocation("US")
              .setCredentials(NoCredentials.getInstance())
              .build()
              .getService();
      for (JsonElement ce : cases) {
        results.add(runCase(bq, ce.getAsJsonObject()));
      }
    } catch (Exception e) {
      String detail = e.getClass().getSimpleName() + ": " + e.getMessage();
      results = new JsonArray();
      for (JsonElement ce : cases) {
        results.add(errorResult(ce.getAsJsonObject().get("name").getAsString(), detail));
      }
    }

    JsonObject report = new JsonObject();
    report.addProperty("language", "java");
    report.add("cases", results);
    Files.write(Paths.get(reportFile), new Gson().toJson(report).getBytes(StandardCharsets.UTF_8));

    int failed = 0;
    for (JsonElement r : results) {
      if (!"pass".equals(r.getAsJsonObject().get("status").getAsString())) {
        failed++;
      }
    }
    System.out.println("java: " + results.size() + " case(s), " + failed + " not passing");
  }

  private static JsonObject runCase(BigQuery bq, JsonObject testCase) {
    String name = testCase.get("name").getAsString();
    try {
      if (testCase.has("setup")) {
        applySetup(bq, testCase.getAsJsonObject("setup"));
      }
      QueryJobConfiguration.Builder cfg =
          QueryJobConfiguration.newBuilder(testCase.get("sql").getAsString());
      if (testCase.has("params")) {
        for (JsonElement pe : testCase.getAsJsonArray("params")) {
          JsonObject p = pe.getAsJsonObject();
          cfg.addNamedParameter(p.get("name").getAsString(), paramValue(p));
        }
      }
      TableResult result = bq.query(cfg.build());
      Schema schema = result.getSchema();
      JsonArray actual = new JsonArray();
      for (FieldValueList row : result.iterateAll()) {
        actual.add(rowToJson(row, schema.getFields()));
      }
      JsonElement expected = testCase.get("expected");
      if (jsonEquals(actual, expected)) {
        return result(name, "pass", "");
      }
      return result(name, "fail", "expected " + expected + " got " + actual);
    } catch (Exception e) {
      return errorResult(name, e.getClass().getSimpleName() + ": " + e.getMessage());
    }
  }

  /**
   * applySetup streams a case's {@code setup.rows} into its target table
   * through tabledata.insertAll, so the query under test runs against
   * freshly streamed data. The dataset and table are preloaded by the test
   * harness (modelling an emulator started with --data-from-yaml); this
   * runner only performs the streaming insert. Regression coverage for issue
   * #470 — streamed rows must be visible to a subsequent query.
   */
  private static void applySetup(BigQuery bq, JsonObject setup) {
    TableId tableId =
        TableId.of(setup.get("dataset").getAsString(), setup.get("table").getAsString());
    InsertAllRequest.Builder insert = InsertAllRequest.newBuilder(tableId);
    for (JsonElement re : setup.getAsJsonArray("rows")) {
      Map<String, Object> row = new LinkedHashMap<>();
      for (Map.Entry<String, JsonElement> field : re.getAsJsonObject().entrySet()) {
        row.put(field.getKey(), field.getValue().getAsString());
      }
      insert.addRow(row);
    }
    InsertAllResponse resp = bq.insertAll(insert.build());
    if (resp.hasErrors()) {
      throw new RuntimeException("insertAll reported errors: " + resp.getInsertErrors());
    }
  }

  private static QueryParameterValue paramValue(JsonObject p) {
    String type = p.get("type").getAsString();
    JsonElement v = p.get("value");
    switch (type) {
      case "INT64":
        return QueryParameterValue.int64(v.getAsLong());
      case "FLOAT64":
        return QueryParameterValue.float64(v.getAsDouble());
      case "BOOL":
        return QueryParameterValue.bool(v.getAsBoolean());
      default:
        return QueryParameterValue.string(v.getAsString());
    }
  }

  private static JsonObject rowToJson(FieldValueList row, FieldList fields) {
    JsonObject obj = new JsonObject();
    for (int i = 0; i < fields.size(); i++) {
      Field field = fields.get(i);
      obj.add(field.getName(), normalize(field, row.get(i)));
    }
    return obj;
  }

  private static JsonElement normalize(Field field, FieldValue fv) {
    if (fv.isNull()) {
      return JsonNull.INSTANCE;
    }
    if (fv.getAttribute() == FieldValue.Attribute.REPEATED) {
      JsonArray arr = new JsonArray();
      for (FieldValue elem : fv.getRepeatedValue()) {
        arr.add(normalizeElement(field, elem));
      }
      return arr;
    }
    return normalizeElement(field, fv);
  }

  private static JsonElement normalizeElement(Field field, FieldValue fv) {
    if (fv.isNull()) {
      return JsonNull.INSTANCE;
    }
    if (fv.getAttribute() == FieldValue.Attribute.RECORD) {
      JsonObject obj = new JsonObject();
      FieldList subs = field.getSubFields();
      FieldValueList rec = fv.getRecordValue();
      for (int i = 0; i < subs.size(); i++) {
        obj.add(subs.get(i).getName(), normalize(subs.get(i), rec.get(i)));
      }
      return obj;
    }
    return normalizePrimitive(field.getType().getStandardType(), fv);
  }

  private static JsonElement normalizePrimitive(StandardSQLTypeName type, FieldValue fv) {
    switch (type) {
      case INT64:
        return new JsonPrimitive(fv.getLongValue());
      case FLOAT64:
        return new JsonPrimitive(fv.getDoubleValue());
      case BOOL:
        return new JsonPrimitive(fv.getBooleanValue());
      case NUMERIC:
      case BIGNUMERIC:
        return new JsonPrimitive(fv.getNumericValue().toPlainString());
      case BYTES:
        return new JsonPrimitive(Base64.getEncoder().encodeToString(fv.getBytesValue()));
      case TIMESTAMP:
        long micros = fv.getTimestampValue();
        Instant inst = Instant.EPOCH.plus(micros, ChronoUnit.MICROS);
        return new JsonPrimitive(TS_FMT.format(inst));
      default:
        return new JsonPrimitive(fv.getStringValue());
    }
  }

  private static boolean jsonEquals(JsonElement actual, JsonElement expected) {
    if (actual.isJsonNull() || expected.isJsonNull()) {
      return actual.isJsonNull() && expected.isJsonNull();
    }
    if (actual.isJsonArray() && expected.isJsonArray()) {
      JsonArray a = actual.getAsJsonArray();
      JsonArray e = expected.getAsJsonArray();
      if (a.size() != e.size()) {
        return false;
      }
      for (int i = 0; i < a.size(); i++) {
        if (!jsonEquals(a.get(i), e.get(i))) {
          return false;
        }
      }
      return true;
    }
    if (actual.isJsonObject() && expected.isJsonObject()) {
      JsonObject a = actual.getAsJsonObject();
      JsonObject e = expected.getAsJsonObject();
      if (!a.keySet().equals(e.keySet())) {
        return false;
      }
      for (String k : e.keySet()) {
        if (!jsonEquals(a.get(k), e.get(k))) {
          return false;
        }
      }
      return true;
    }
    if (actual.isJsonPrimitive() && expected.isJsonPrimitive()) {
      JsonPrimitive a = actual.getAsJsonPrimitive();
      JsonPrimitive e = expected.getAsJsonPrimitive();
      if (a.isNumber() && e.isNumber()) {
        return Math.abs(a.getAsDouble() - e.getAsDouble()) <= 1e-9;
      }
      return a.equals(e);
    }
    return false;
  }

  private static JsonObject result(String name, String status, String detail) {
    JsonObject o = new JsonObject();
    o.addProperty("name", name);
    o.addProperty("status", status);
    o.addProperty("detail", detail);
    return o;
  }

  private static JsonObject errorResult(String name, String detail) {
    return result(name, "error", detail);
  }

  private static String envOr(String key, String fallback) {
    String v = System.getenv(key);
    return (v == null || v.isEmpty()) ? fallback : v;
  }

  private Runner() {}
}
