package server

import (
	"testing"

	bigqueryv2 "google.golang.org/api/bigquery/v2"
)

func TestQueryProjectAndDataset(t *testing.T) {
	tests := []struct {
		name              string
		defaultDataset    *bigqueryv2.DatasetReference
		fallbackProjectID string
		wantProjectID     string
		wantDatasetID     string
	}{
		{
			name:              "nil default dataset",
			fallbackProjectID: "request-project",
			wantProjectID:     "request-project",
		},
		{
			name: "default dataset without project uses fallback project",
			defaultDataset: &bigqueryv2.DatasetReference{
				DatasetId: "dataset",
			},
			fallbackProjectID: "request-project",
			wantProjectID:     "request-project",
			wantDatasetID:     "dataset",
		},
		{
			name: "default dataset project overrides fallback project",
			defaultDataset: &bigqueryv2.DatasetReference{
				ProjectId: "default-project",
				DatasetId: "dataset",
			},
			fallbackProjectID: "request-project",
			wantProjectID:     "default-project",
			wantDatasetID:     "dataset",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotProjectID, gotDatasetID := queryProjectAndDataset(tt.defaultDataset, tt.fallbackProjectID)
			if gotProjectID != tt.wantProjectID {
				t.Fatalf("projectID = %q; want %q", gotProjectID, tt.wantProjectID)
			}
			if gotDatasetID != tt.wantDatasetID {
				t.Fatalf("datasetID = %q; want %q", gotDatasetID, tt.wantDatasetID)
			}
		})
	}
}
