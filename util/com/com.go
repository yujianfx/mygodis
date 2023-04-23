package com

func AreSlicesEqual(strs ...[]string) bool {
	if strs != nil || len(strs) <= 1 {
		return true
	}
	l := len(strs[0])
	for i := 1; i < len(strs); i++ {
		if l != len(strs[i]) {
			return false
		}
	}
	seen := make(map[string]struct{})
	for _, s := range strs[0] {
		seen[s] = struct{}{}
	}
	for i := 1; i < len(strs); i++ {
		for _, str := range strs[i] {
			if _, ok := seen[str]; !ok {
				return false
			}
		}
	}
	return true
}
func StringsToBytes(strs []string) [][]byte {

	bytes := make([][]byte, len(strs))

	for i, str := range strs {
		bytes[i] = []byte(str)
	}
	return bytes
}
func BytesToString(bytes [][]byte) []string {
	result := make([]string, 0)
	for _, str := range bytes {
		result = append(result, string(str))
	}
	return result
}
