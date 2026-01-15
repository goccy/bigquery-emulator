package internal

import (
	"math"
	"testing"
)

func Test_FunctionGeography_ST_GEOGPOINT(t *testing.T) {
	t.Parallel()

	t.Run("ST_GEOGPOINT OK", func(t *testing.T) {
		t.Parallel()

		v, err := ST_GEOGPOINT(10, 20)
		if err != nil {
			t.Fatal(err)
		}

		res, err := v.ToString()
		if err != nil {
			t.Fatal(err)
		}

		if res != "POINT (10 20)" {
			t.Fatalf("unexpected result: %s", res)
		}
	})

	t.Run("ST_GEOGPOINT latitude boundaries", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			lat float64
		}{
			{-90},
			{90},
		}

		for _, tt := range tests {
			_, err := ST_GEOGPOINT(0, tt.lat)
			if err != nil {
				t.Fatalf("unexpected error for latitude %f: %v", tt.lat, err)
			}
		}
	})

	t.Run("ST_GEOGPOINT invalid latitude", func(t *testing.T) {
		t.Parallel()

		tests := []float64{
			-90.0001,
			90.0001,
			100,
			-100,
		}

		for _, lat := range tests {
			_, err := ST_GEOGPOINT(0, lat)
			if err == nil {
				t.Fatalf("expected error for latitude %f", lat)
			}
		}
	})

	t.Run("ST_GEOGPOINT normalizes longitude", func(t *testing.T) {
		t.Parallel()

		v, err := ST_GEOGPOINT(190, 0)
		if err != nil {
			t.Fatal(err)
		}

		res, err := v.ToString()
		if err != nil {
			t.Fatal(err)
		}

		// 190 -> -170
		if res != "POINT (-170 0)" {
			t.Fatalf("unexpected result: %s", res)
		}
	})
}

func Test_FunctionGeography_geographyNormalizeLongitude(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    float64
		expected float64
	}{
		{"zero", 0, 0},
		{"within range positive", 45, 45},
		{"within range negative", -45, -45},
		{"exact 180", 180, 180},
		{"exact -180", -180, -180},
		{"over 180", 190, -170},
		{"under -180", -190, 170},
		{"full rotation positive", 360, 0},
		{"full rotation negative", -360, 0},
		{"multiple rotations", 540, -180},
		{"large positive", 1080 + 30, 30},
		{"large negative", -1080 - 30, -30},
	}

	for _, tt := range tests {
		tt := tt // capture
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			res := geographyNormalizeLongitude(tt.input)
			if res != tt.expected {
				t.Fatalf(
					"unexpected result for %f: got %f, expected %f",
					tt.input, res, tt.expected,
				)
			}
		})
	}
}

func Test_FunctionGeography_ST_GEOGFROMTEXT(t *testing.T) {
	t.Parallel()

	t.Run("ST_GEOGFROMTEXT OK", func(t *testing.T) {
		t.Parallel()

		val, err := ST_GEOGFROMTEXT("POINT(10 24.3)")
		if err != nil {
			t.Fatal(err)
		}

		res, err := val.ToString()
		if err != nil {
			t.Fatal(err)
		}

		if res != "POINT (10 24.3)" {
			t.Fatalf("unexpected result: %s", res)
		}
	})

	t.Run("ST_GEOGFROMTEXT invalid WKT", func(t *testing.T) {
		t.Parallel()

		_, err := ST_GEOGFROMTEXT("LINESTRING (0 0, 1 1)")
		if err == nil {
			t.Fatal("expected error")
		}
	})
}

func Test_FunctionGeography_ST_DISTANCE(t *testing.T) {
	t.Parallel()

	t.Run("ST_DISTANCE OK, > 0", func(t *testing.T) {
		t.Parallel()

		p1 := NewGeographyPoint(100, 90)
		p2 := NewGeographyPoint(100.03, 89.999)

		val, err := ST_DISTANCE(p1, p2)
		if err != nil {
			t.Fatal(err)
		}

		dist, err := val.ToFloat64()
		if err != nil {
			t.Fatal(err)
		}

		testGeographyAssertDistanceEqual(t, dist, 111.19510117719409)
	})

	t.Run("ST_DISTANCE OK, 0 distance", func(t *testing.T) {
		t.Parallel()

		p1 := NewGeographyPoint(100.03, 89.999)
		p2 := NewGeographyPoint(100.03, 89.999)

		val, err := ST_DISTANCE(p1, p2)
		if err != nil {
			t.Fatal(err)
		}

		dist, err := val.ToFloat64()
		if err != nil {
			t.Fatal(err)
		}

		testGeographyAssertDistanceEqual(t, dist, 0)
	})

	t.Run("ST_DISTANCE bad arguments", func(t *testing.T) {
		t.Parallel()

		_, err := ST_DISTANCE(nil, nil)
		if err == nil {
			t.Fatal(err)
		}
	})
}

func testGeographyAssertDistanceEqual(t *testing.T, dist, expected float64) {
	const threshold = 5.0

	if math.Abs(dist-expected) >= threshold {
		t.Fatalf("expected distance close to %f meters, got %f", expected, dist)
	}
}
