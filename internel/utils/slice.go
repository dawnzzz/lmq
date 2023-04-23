package utils

func Uniq(s []string) (r []string) {
outerLoop:
	for _, entry := range s {
		for _, existing := range r {
			if existing == entry {
				continue outerLoop
			}
		}
		r = append(r, entry)
	}
	return
}
