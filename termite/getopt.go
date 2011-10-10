package termite
import (
	"log"
	"strings"
)

type GetoptResult struct {
	Long  map[string]string 
	Short map[byte]string
	Args  []string
}

func Getopt(args []string, longTakeArg []string, shortTakeArg []byte, reorder bool) (r GetoptResult) {
	longOpts := map[string]int{}
	for _, v := range longTakeArg {
		longOpts[v] = 1
	}
	
	shOpts := map[byte]int{}
	for _, v := range shortTakeArg {
		shOpts[v] = 1
	}
	
	r = GetoptResult{
		Long: map[string]string{},
		Short: map[byte]string{},
	}

	var nextShortArg byte
	var nextLongArg string
	for i, a := range args {
		switch {
		case nextLongArg != "":
			r.Long[nextLongArg] = a
			nextLongArg = ""
		case nextShortArg != 0:
			r.Short[nextShortArg] = a
			nextShortArg = 0
		case a == "--":
			r.Args = args[i+1:]
			return r
		case len(a) > 2 && a[:2] == "--":
			name := a[2:]
			if strings.Contains(name, "=") {
				comps := strings.SplitN(name, "=", 2)
				if _, ok := longOpts[comps[0]]; ok {
					r.Long[comps[0]] = comps[1]
				} else {
					r.Long[name] = ""
				}
			} else {
				if _, ok := longOpts[name]; ok {
					nextLongArg = name
				} else {
					r.Long[name] = ""
				}
			}
		case len(a) > 1 && a[0] == '-':
			for j, oInt := range a[1:] {
				o := byte(oInt)
				if _, ok := shOpts[o]; ok {
					if j == len(a) - 2 {
						nextShortArg = o
					} else {
						r.Short[o] = a[j+2:]
						break
					}
				} else {
					r.Short[o] = ""
				}
			}
		default:
			
			if reorder {
				r.Args = append(r.Args)
			} else {
				r.Args = args[i:]
				return r
			}
		}
	}

	// TODO - should signal error if nextShortArg, nextLongArg set.
	return r
}
