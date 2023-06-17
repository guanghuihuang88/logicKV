package utils

import "strconv"

func FloatFromBytes(val []byte) float64 {
	float, _ := strconv.ParseFloat(string(val), 64)
	return float
}

func Float64ToBytes(val float64) []byte {
	return []byte(strconv.FormatFloat(val, 'f', -1, 64))
}
