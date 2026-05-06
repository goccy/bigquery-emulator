package zetasqlite

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestNormalizeTablePath(t *testing.T) {
	c := &Catalog{}

	tests := []struct {
		name     string
		input    []string
		expected []string
	}{
		{
			name:     "already normalized path",
			input:    []string{"project", "dataset", "table"},
			expected: []string{"project", "dataset", "table"},
		},
		{
			name:     "single element with dots",
			input:    []string{"project.dataset.table"},
			expected: []string{"project", "dataset", "table"},
		},
		{
			name:     "mixed normalized and dot-separated",
			input:    []string{"project", "dataset.table"},
			expected: []string{"project", "dataset", "table"},
		},
		{
			name:     "two element path",
			input:    []string{"dataset", "table"},
			expected: []string{"dataset", "table"},
		},
		{
			name:     "single element no dots",
			input:    []string{"table"},
			expected: []string{"table"},
		},
		{
			name:     "empty path",
			input:    []string{},
			expected: []string{},
		},
		{
			name:     "project with hyphen",
			input:    []string{"my-project.dataset.table"},
			expected: []string{"my-project", "dataset", "table"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := c.normalizeTablePath(tt.input)
			if diff := cmp.Diff(tt.expected, result); diff != "" {
				t.Errorf("normalizeTablePath(%v) mismatch (-want +got):\n%s", tt.input, diff)
			}
		})
	}
}
