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

	file, err := os.OpenFile("test.log", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		panic(err)
	}
	log.SetOutput(file)

}

func writeBench(b *testing.B, keyMod int) {

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
		err := benchStore.WriteToKey(strconv.Itoa(i%keyMod), blobs[i])
		if err != nil {
			b.Fatal("Failed to write to store:", err)
		}
	}
	b.StopTimer()
	os.RemoveAll(benchDir)

}
func BenchmarkCommonKey10(b *testing.B) {
	writeBench(b, 10)
}

func BenchmarkUniqueKeys(b *testing.B) {
	writeBench(b, b.N)
}
