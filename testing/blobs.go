package testing

import (
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"text/template"
	"time"
)

type blob struct {
	Created  string
	User     string
	Field    string
	NewValue string
	OldValue string
}

var blobTemplate = `{"created":"{{.Created}}","user":"{{.User}}","property":"description","oldValue":"{{.OldValue}}","newValue":"{{.NewValue}}"}`
var tmpl = template.Must(template.New("blob").Parse(blobTemplate))

var blobs [][]byte
var users []string = []string{
	"bob@test.com",
	"jane@test.com",
	"chris@test.com",
	"cathy@test.com",
}

func GenerateBlobs(count int) [][]byte {

	log.Println("Generating blobs ...")
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	blobs := [][]byte{}

	step := 100
	if count > 1000 && count < 100000 {
		step = 1000
	}
	if count >= 100000 {
		step = 10000
	}

	for i := 0; i < count; i++ {
		b := &blob{
			Created:  time.Now().Format(time.RFC3339Nano),
			User:     users[r.Intn(len(users))],
			Field:    "description",
			NewValue: fmt.Sprintf("This is the description witih a bit of random data added: %d", r.Int63()),
			OldValue: fmt.Sprintf("This is the old description with a bit of random data added: %d", r.Int63()),
		}

		buf := &bytes.Buffer{}
		err := tmpl.Execute(buf, b)
		if err != nil {
			log.Fatal("Failed to generate a template value:", err)
		}
		blobs = append(blobs, buf.Bytes())
		if i%step == 0 {
			log.Println("Blobs:", i)
		}
	}
	log.Println("Done. Blobs:", count)
	return blobs
}
