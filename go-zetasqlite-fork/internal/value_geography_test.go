package internal

import "testing"

func Test_ValueGeography_GeographyFromWKT(t *testing.T) {
	t.Parallel()

	t.Run("POINT OK", func(t *testing.T) {
		t.Parallel()

		g, err := GeographyFromWKT("POINT (-1 2)")
		if err != nil {
			t.Fatal(err)
		}

		wkt, err := g.g.ToWKT()
		if err != nil {
			t.Fatal(err)
		}

		if wkt != "POINT (-1 2)" {
			t.Fatalf("unexpected WKT: %s", wkt)
		}
	})

	t.Run("POINT lowercase and spaces", func(t *testing.T) {
		t.Parallel()

		g, err := GeographyFromWKT("   point (  -10.5   42 ) ")
		if err != nil {
			t.Fatal(err)
		}

		wkt, err := g.g.ToWKT()
		if err != nil {
			t.Fatal(err)
		}

		if wkt != "POINT (-10.5 42)" {
			t.Fatalf("unexpected WKT: %s", wkt)
		}
	})
}

func Test_ValueGeography_GeographyFromWKT_Invalid(t *testing.T) {
	t.Parallel()

	cases := []string{
		"",
		"POINT",
		"POINT ()",
		"POINT (1)",
		"POINT (1 2 3)",
		"POINT (a b)",
		"LINESTRING (1 2)",
	}

	for _, tc := range cases {
		tc := tc

		t.Run(tc, func(t *testing.T) {
			t.Parallel()

			_, err := GeographyFromWKT(tc)
			if err == nil {
				t.Fatalf("expected error for input: %q", tc)
			}
		})
	}
}
