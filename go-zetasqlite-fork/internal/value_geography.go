package internal

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"
)

// GeographyValue is based on the OGC Simple Features specification (SFS),
// and can contain a set of Geography objects.
// The underlying g geographyType is responsible for the specific Geography object implementation.
type GeographyValue struct {
	g geographyType
}

// geographyType interface is responsible for the specific Geography object implementation.
// The only implementation supported at the moment is a single Point (geographyPoint type).
// ToDo: implement other geographyType implementations: multipoint, linestring, ...
type geographyType interface {
	ToWKT() (string, error)
}

func NewGeographyPoint(longitude, latitude float64) *GeographyValue {
	return &GeographyValue{
		g: &geographyPoint{
			longitude: geographyNormalizeLongitude(longitude),
			latitude:  latitude,
		},
	}
}

// GeographyFromWKT parses a GeographyValue object from the Well-known Text (WKT) format.
// Now it supports the syntax for geo points only: "POINT (lon lat)"
// ToDo: support complete WKT parsing.
func GeographyFromWKT(wkt string) (*GeographyValue, error) {
	res, err := geographyPointFromWKT(wkt)
	if err != nil {
		return nil, fmt.Errorf("unsupported Geopraphy WKT text: %q, err: %w", wkt, err)
	}

	return res, nil
}

func (g *GeographyValue) Add(_ Value) (Value, error) {
	return nil, fmt.Errorf("unsupported add operator for geography value")
}

func (g *GeographyValue) Sub(_ Value) (Value, error) {
	return nil, fmt.Errorf("unsupported sub operator for geography value")
}

func (g *GeographyValue) Mul(_ Value) (Value, error) {
	return nil, fmt.Errorf("unsupported mul operator for geography value")
}

func (g *GeographyValue) Div(_ Value) (Value, error) {
	return nil, fmt.Errorf("unsupported div operator for geography value")
}

func (g *GeographyValue) EQ(_ Value) (bool, error) {
	return false, fmt.Errorf("unsupported eq operator for geography value")
}

func (g *GeographyValue) GT(_ Value) (bool, error) {
	return false, fmt.Errorf("unsupported gt operator for geography value")
}

func (g *GeographyValue) GTE(_ Value) (bool, error) {
	return false, fmt.Errorf("unsupported gte operator for geography value")
}

func (g *GeographyValue) LT(_ Value) (bool, error) {
	return false, fmt.Errorf("unsupported lt operator for geography value")
}

func (g *GeographyValue) LTE(_ Value) (bool, error) {
	return false, fmt.Errorf("unsupported lte operator for geography value")
}

func (g *GeographyValue) ToInt64() (int64, error) {
	return 0, fmt.Errorf("unsupported ToInt64 operator for geography value")
}

// ToWKT returns a representation of the Geography object in the Well-known Text (WKT) format.
func (g *GeographyValue) ToWKT() (string, error) {
	return g.g.ToWKT()
}

func (g *GeographyValue) ToString() (string, error) {
	return g.ToWKT()
}

func (g *GeographyValue) String() (string, error) {
	return g.ToWKT()
}

func (g *GeographyValue) ToBytes() ([]byte, error) {
	v, err := g.ToString()
	if err != nil {
		return nil, err
	}

	return []byte(v), nil
}

func (g *GeographyValue) ToFloat64() (float64, error) {
	return 0, fmt.Errorf("unsupported ToFloat64 operator for geography value")
}

func (g *GeographyValue) ToBool() (bool, error) {
	return false, fmt.Errorf("unsupported ToBool operator for geography value")
}

func (g *GeographyValue) ToArray() (*ArrayValue, error) {
	return nil, fmt.Errorf("unsupported ToArray operator for geography value")
}

func (g *GeographyValue) ToStruct() (*StructValue, error) {
	return nil, fmt.Errorf("unsupported ToStruct operator for geography value")
}

func (g *GeographyValue) ToJSON() (string, error) {
	s, err := g.ToString()
	if err != nil {
		return "", err
	}

	return strconv.Quote(s), nil
}

func (g *GeographyValue) ToTime() (time.Time, error) {
	return time.Time{}, fmt.Errorf("unsupported ToTime operator for geography value")
}

func (g *GeographyValue) ToRat() (*big.Rat, error) {
	return nil, fmt.Errorf("unsupported ToRat operator for geography value")
}

func (g *GeographyValue) Format(verb rune) string {
	str, err := g.ToString()
	if err != nil {
		return "error"
	}

	switch verb {
	case 't':
		return str
	case 'T':
		return fmt.Sprintf(`GEOGRAPHY %q`, str)
	}

	return str
}

func (g *GeographyValue) Interface() interface{} {
	s, err := g.ToString()
	if err != nil {
		return nil
	}

	return s
}

type geographyPoint struct {
	longitude float64
	latitude  float64
}

func (g *geographyPoint) ToWKT() (string, error) {
	lon := strconv.FormatFloat(g.longitude, 'f', -1, 64)
	lat := strconv.FormatFloat(g.latitude, 'f', -1, 64)
	return fmt.Sprintf("POINT (%s %s)", lon, lat), nil
}

// geographyNormalizeLongitude uses the input longitude modulo 360 to obtain a longitude within [-180, 180].
func geographyNormalizeLongitude(longitude float64) float64 {
	if -180 <= longitude && longitude <= 180 {
		return longitude
	}

	longitude = math.Mod(longitude+180, 360)
	if longitude < 0 {
		longitude += 360
	}

	return longitude - 180
}

func geographyPointFromWKT(wkt string) (*GeographyValue, error) {
	wkt = strings.TrimSpace(wkt)
	wkt = strings.ToUpper(wkt)

	if !strings.HasPrefix(wkt, "POINT") {
		return nil, errors.New("not a POINT WKT")
	}

	start := strings.Index(wkt, "(")
	end := strings.Index(wkt, ")")
	if start == -1 || end == -1 || end <= start {
		return nil, errors.New("invalid POINT format")
	}

	coords := strings.Fields(wkt[start+1 : end])
	if len(coords) != 2 {
		return nil, errors.New("POINT must contain exactly 2 coordinates")
	}

	lon, err := strconv.ParseFloat(coords[0], 64)
	if err != nil {
		return nil, err
	}

	lat, err := strconv.ParseFloat(coords[1], 64)
	if err != nil {
		return nil, err
	}

	return NewGeographyPoint(lon, lat), nil
}
