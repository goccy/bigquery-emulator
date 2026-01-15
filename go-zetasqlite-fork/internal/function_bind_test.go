package internal

import "testing"

func Test_FunctionBind_bindStGeogPoint(t *testing.T) {
	t.Parallel()

	t.Run("bindStGeogPoint OK", func(t *testing.T) {
		t.Parallel()

		point, err := bindStGeogPoint(FloatValue(1), FloatValue(2))
		if err != nil {
			t.Fatal(err)
		}

		res, err := point.ToString()
		if err != nil {
			t.Fatal(err)
		}

		if res != "POINT (1 2)" {
			t.Fatalf("unexpected result: %s", res)
		}
	})

	t.Run("bindStGeogPoint bad arguments", func(t *testing.T) {
		t.Parallel()

		_, err := bindStGeogPoint(FloatValue(1))
		if err == nil {
			t.Fatal("expected error")
		}
	})
}

func Test_FunctionBind_bindStGeogFromText(t *testing.T) {
	t.Parallel()

	t.Run("bindStGeogFromText OK", func(t *testing.T) {
		t.Parallel()

		point, err := bindStGeogFromText(StringValue("POINT (0 -49.23)"))
		if err != nil {
			t.Fatal(err)
		}

		res, err := point.ToString()
		if err != nil {
			t.Fatal(err)
		}

		if res != "POINT (0 -49.23)" {
			t.Fatalf("unexpected result: %s", res)
		}
	})

	t.Run("bindStGeogFromText bad arguments", func(t *testing.T) {
		t.Parallel()

		_, err := bindStGeogFromText(FloatValue(1))
		if err == nil {
			t.Fatal("expected error")
		}
	})
}

func Test_FunctionBind_bindStDistance(t *testing.T) {
	t.Parallel()

	t.Run("StDistance OK", func(t *testing.T) {
		t.Parallel()

		dist, err := bindStDistance(NewGeographyPoint(1, 2), NewGeographyPoint(2, 3))
		if err != nil {
			t.Fatal(err)
		}

		res, err := dist.ToFloat64()
		if err != nil {
			t.Fatal(err)
		}

		if res <= 0 {
			t.Fatalf("unexpected result: %f", res)
		}
	})

	t.Run("bindStDistance bad arguments", func(t *testing.T) {
		t.Parallel()

		_, err := bindStDistance(FloatValue(1))
		if err == nil {
			t.Fatal("expected error")
		}
	})
}
