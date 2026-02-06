package internal

import (
	"testing"
)

func TestToLocation(t *testing.T) {
	t.Run("+09", func(t *testing.T) {
		if _, err := toLocation("+09"); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("+09:00", func(t *testing.T) {
		if _, err := toLocation("+09:00"); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("-09", func(t *testing.T) {
		if _, err := toLocation("-09"); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("-09:00", func(t *testing.T) {
		if _, err := toLocation("-09:00"); err != nil {
			t.Fatal(err)
		}
	})
}
