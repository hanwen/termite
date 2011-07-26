#!/bin/sh

VERSION=\"$(git log -n1 --pretty=format:'%h (%cd)' --date=iso )\"

cat <<EOF
package termite
func init() {
	version = new(string)
	*version = ${VERSION}
}
EOF
