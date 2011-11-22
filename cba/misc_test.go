package cba

import (
	"io/ioutil"
	"os"
	"testing"
)

func TestReadHexDatabase(t *testing.T) {
	d, _ := ioutil.TempDir("", "termite")
	sd := d + "/ab"
	os.Mkdir(sd, 0755)
	ioutil.WriteFile(sd+"/cd", []byte{42}, 0644)
	ioutil.WriteFile(sd+"/df", []byte{42}, 0644)

	db := ReadHexDatabase(d)

	if len(db) != 2 || !db["\xab\xcd"] || !db["\xab\xdf"] {
		t.Fatalf("ReadHexDatabase() want [0xabcd 0xabdf], got", db)
	}
}
