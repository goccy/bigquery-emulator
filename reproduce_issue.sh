#!/bin/bash

PROJECT_ID="test"
DATASET_ID="test"
TABLE_ID="ad_buckets_1767868700"
VIEW_ID="ad_buckets_view"

# Create Dataset
curl -X POST http://localhost:9050/bigquery/v2/projects/$PROJECT_ID/datasets \
-H "Content-Type: application/json" \
-d "{
  \"datasetReference\": {
    \"datasetId\": \"$DATASET_ID\",
    \"projectId\": \"$PROJECT_ID\"
  }
}"

# Create Table
curl -X POST http://localhost:9050/bigquery/v2/projects/$PROJECT_ID/datasets/$DATASET_ID/tables \
-H "Content-Type: application/json" \
-d "{
  \"tableReference\": {
    \"datasetId\": \"$DATASET_ID\",
    \"projectId\": \"$PROJECT_ID\",
    \"tableId\": \"$TABLE_ID\"
  },
  \"schema\": {
    \"fields\": [
      {\"name\": \"id\", \"type\": \"INTEGER\"},
      {\"name\": \"name\", \"type\": \"STRING\"}
    ]
  }
}"

# Insert Data
curl -X POST http://localhost:9050/bigquery/v2/projects/$PROJECT_ID/datasets/$DATASET_ID/tables/$TABLE_ID/insertAll \
-H "Content-Type: application/json" \
-d "{
  \"rows\": [
    {\"json\": {\"id\": 1, \"name\": \"foo\"}},
    {\"json\": {\"id\": 2, \"name\": \"bar\"}}
  ]
}"

# Create View
curl -X POST http://localhost:9050/bigquery/v2/projects/$PROJECT_ID/datasets/$DATASET_ID/tables \
-H "Content-Type: application/json" \
-d "{
  \"tableReference\": {
    \"datasetId\": \"$DATASET_ID\",
    \"projectId\": \"$PROJECT_ID\",
    \"tableId\": \"$VIEW_ID\"
  },
  \"view\": {
    \"query\": \"SELECT * FROM \`$PROJECT_ID.$DATASET_ID.$TABLE_ID\`\"
  }
}"

# Query View
curl -X POST http://localhost:9050/projects/$PROJECT_ID/queries \
-H "Content-Type: application/json" \
-d "{
  \"query\": \"SELECT COUNT(*) FROM \`$PROJECT_ID.$DATASET_ID.$VIEW_ID\`\",
  \"useLegacySql\": false
}"