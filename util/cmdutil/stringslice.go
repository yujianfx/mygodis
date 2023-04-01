package cmdutil

func ContainsString(slice []string, target string) bool {
	for _, s := range slice {
		if s == target {
			return true
		}
	}
	return false
}
func ContainsStrings(slice []string, targets ...string) bool {
	for _, target := range targets {
		if !ContainsString(slice, target) {
			return false
		}
	}
	return true
}
