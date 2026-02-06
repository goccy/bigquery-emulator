package internal

import (
	"errors"
	"fmt"
	"math"
)

// ST_GEOGPOINT returns a Geography Point with provided longitude and latitude.
func ST_GEOGPOINT(longitude float64, latitude float64) (Value, error) {
	if latitude < -90 || latitude > 90 {
		return nil, fmt.Errorf("ST_GEOGPOINT: invalid latitude: %f", latitude)
	}

	return NewGeographyPoint(longitude, latitude), nil
}

// ST_GEOGFROMTEXT parses a WKT string into a Geography object.
// Now supports geographyPoint type only.
// ToDo: support other geo types in ST_GEOGFROMTEXT.
func ST_GEOGFROMTEXT(wkt string) (Value, error) {
	res, err := GeographyFromWKT(wkt)
	if err != nil {
		return nil, fmt.Errorf("ST_GEOGFROMTEXT failed: %w", err)
	}

	return res, nil
}

// ST_DISTANCE is a standard Haversine implementation for geo distance.
// Returns Float.
// Now supports geographyPoint arguments only.
// ToDo: support other geo types in ST_DISTANCE.
func ST_DISTANCE(geo1 *GeographyValue, geo2 *GeographyValue) (Value, error) {
	if geo1 == nil || geo2 == nil {
		return nil, errors.New("nil geography")
	}

	p1, ok := geo1.g.(*geographyPoint)
	if !ok {
		return nil, errors.New("unsupported geography type for geo1")
	}

	p2, ok := geo2.g.(*geographyPoint)
	if !ok {
		return nil, errors.New("unsupported geography type for geo2")
	}

	dist := haversineDistance(
		p1.latitude,
		p1.longitude,
		p2.latitude,
		p2.longitude,
	)

	return FloatValue(dist), nil
}

func haversineDistance(lat1, lon1, lat2, lon2 float64) float64 {
	const earthRadiusMeters = 6371000.0

	lat1Rad := degToRad(lat1)
	lon1Rad := degToRad(lon1)
	lat2Rad := degToRad(lat2)
	lon2Rad := degToRad(lon2)

	dLat := lat2Rad - lat1Rad
	dLon := lon2Rad - lon1Rad

	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1Rad)*math.Cos(lat2Rad)*
			math.Sin(dLon/2)*math.Sin(dLon/2)

	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))

	return earthRadiusMeters * c
}

func degToRad(deg float64) float64 {
	return deg * math.Pi / 180
}
