package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"testing"
	"time"
)

const (
	DEFAULT_PORT     = "localhost:9898"
	ASTORED_PORT_ENV = "ASTORED_PORT"
)

var astored_host string = DEFAULT_PORT

func init() {
	if os.Getenv(ASTORED_PORT_ENV) != "" {
		astored_host = os.Getenv(ASTORED_PORT_ENV)
	}
}

func BenchmarkHTTP(b *testing.B) {

	key := fmt.Sprintf("%s-%d", time.Now().Format(time.RFC822), b.N)

	client := &http.Client{}

	for i := 0; i < b.N; i++ {
		Append(client, key, []byte(fmt.Sprintf("data %d", i)))
	}
}

func BenchmarkHTTPParallel(b *testing.B) {

	key := fmt.Sprintf("%s-%d", time.Now().Format(time.RFC822), b.N)

	client := &http.Client{}
	b.RunParallel(func(pb *testing.PB) {
		var i int
		for pb.Next() {
			i++
			Append(client, key, []byte(fmt.Sprintf("data %s %d", time.Now().Format(time.RFC3339), i)))
		}
	})
}

func Append(client *http.Client, key string, data []byte) {

	url := fmt.Sprintf("http://%s/v1/keys/%s", astored_host, key)

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(data))
	if err != nil {
		log.Fatal("Request error:", err)
	}
	req.Header.Add("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		log.Fatal("Do error:", err)
	}

	var body []byte

	if resp.Body != nil {
		defer resp.Body.Close()
		body, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Fatal("error reading response body:", err)
		}
	}
	if resp.StatusCode >= 400 {
		log.Fatalf("Error response: %d %s %s", resp.StatusCode, resp.Status, body)
	}
}
