package types

import bigqueryv2 "google.golang.org/api/bigquery/v2"

type (
	GetQueryResultsResponse struct {
		JobReference *bigqueryv2.JobReference `json:"jobReference,omitempty"`
		Schema       *bigqueryv2.TableSchema  `json:"schema,omitempty"`
		Rows         []*TableRow              `json:"rows,omitempty"`
		TotalRows    uint64                   `json:"totalRows,omitempty,string"`
		JobComplete  bool                     `json:"jobComplete,omitempty"`
	}

	QueryResponse struct {
		JobReference *bigqueryv2.JobReference `json:"jobReference,omitempty"`
		Schema       *bigqueryv2.TableSchema  `json:"schema,omitempty"`
		Rows         []*TableRow              `json:"rows,omitempty"`
		TotalRows    uint64                   `json:"totalRows,omitempty,string"`
		JobComplete  bool                     `json:"jobComplete,omitempty"`
	}

	TableRow struct {
		F []*TableCell `json:"f,omitempty"`
	}

	// Redefines the TableCell type to return null explicitly
	// because TableCell for bigqueryv2 is omitted if V is nil,
	TableCell struct {
		V interface{} `json:"v"`
	}
)
