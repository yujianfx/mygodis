package com

import "reflect"

func AreSlicesEqual(slices ...[]string) bool {
	if len(slices) == 0 {
		return true
	}
	refSlice := slices[0]
	for _, slice := range slices[1:] {
		if !reflect.DeepEqual(refSlice, slice) {
			return false
		}
	}
	return true
}
func StringsToBytes(strs []string) [][]byte {
	result := make([][]byte, 0)
	for _, str := range strs {
		result = append(result, []byte(str))
	}
	return result
}
func BytesToString(bytes [][]byte) []string {
	result := make([]string, 0)
	for _, str := range bytes {
		result = append(result, string(str))
	}
	return result
}
