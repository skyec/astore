package storebench

import (
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"testing"

	"github.com/skyec/astore"
	blobs "github.com/skyec/astore/testing"
)

var benchStore astore.WriteableStore
var benchDir string

func init() {
}
func BenchmarkDefaultWrite(b *testing.B) {
	benchDir, err := ioutil.TempDir("", "astore-benchmarking-")
	if err != nil {
		log.Fatal(err)
	}

	benchStore, err := astore.NewReadWriteableStore(benchDir)
	if err != nil {
		log.Fatal(err)

	}

	err = benchStore.Initialize()
	if err != nil {
		log.Fatal("Failed to initialze the store:", err)
	}

	blobs := blobs.GenerateBlobs(b.N)
	b.ResetTimer()
	for i := 0; i < len(blobs); i++ {
		err := benchStore.WriteToKey(strconv.Itoa(i%10), blobs[i])
		if err != nil {
			b.Fatal("Failed to write to store:", err)
		}
	}
	b.StopTimer()
	os.RemoveAll(benchDir)

}
