package server

import "testing"

func TestTerminalTableID(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "empty", in: "", want: ""},
		{name: "table only", in: "users", want: "users"},
		{name: "dataset table", in: "sales.users", want: "users"},
		{name: "project dataset table", in: "myproj:sales.users", want: "users"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := terminalTableID(tt.in)
			if got != tt.want {
				t.Fatalf("terminalTableID(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}
