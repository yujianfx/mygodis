package match

import (
	"regexp"
	"strings"
)

func MatchPattern(pattern string, key string) bool {
	if pattern == "*" {
		return true
	}
	pattern = "^" + regexp.QuoteMeta(pattern)
	pattern = strings.Replace(pattern, `\*`, ".*", -1) + "$"
	matched, err := regexp.MatchString(pattern, key)
	if err != nil {
		return false
	}
	return matched
}
