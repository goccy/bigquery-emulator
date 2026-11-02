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

func TestAnonymousDatasetID(t *testing.T) {
	tests := map[string]string{
		"job_123":                              "_job_123",
		"3f2c1d9e-8a4b-4c1f-9d7e-0b6a5c4d3e2f": "_3f2c1d9e_8a4b_4c1f_9d7e_0b6a5c4d3e2f",
		"Mixed-Case_9":                         "_Mixed_Case_9",
	}
	for jobID, want := range tests {
		if got := anonymousDatasetID(jobID); got != want {
			t.Errorf("anonymousDatasetID(%q) = %q, want %q", jobID, got, want)
		}
	}
}
