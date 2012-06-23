package fastpath

func Join(a string, b string) string {
	end := len(a) - 1
	for end >= 0 && a[end] == '/' {
		end--
	}
	end++
	if end == 0 {
		return b
	}
	beg := 0
	for beg < len(b) && b[beg] == '/' {
		beg++
	}
	d := make([]byte, end+len(b)-beg+1)
	copy(d, a[:end])
	d[end] = '/'
	copy(d[end+1:], b[beg:])
	return string(d)
}
