package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch"
)

const esHost = "127.0.0.1:9200"
const timeoutSeconds = 2 * 60

func main() {
	// Wait for cluster to be ready
	startTime := time.Now().Unix()
	timeoutReached := false

	for !timeoutReached {
		res, err := http.Get(fmt.Sprintf("http://%s/_cat/health", esHost))
		if err != nil {
			fmt.Printf("The HTTP request failed with error %s\n", err)
		} else {
			data, _ := ioutil.ReadAll(res.Body)
			stringSlice := strings.Split(string(data), " ")
			// contains() is a little safer than a regex check
			if contains(stringSlice, "green") {
				fmt.Println("ES ready for indexing!")
				break
			}
		}
		fmt.Println("Waiting for Elasticsearch to be ready for requests...")
		time.Sleep(5 * time.Second)
		timeoutReached = (time.Now().Unix() - startTime) > timeoutSeconds
	}

	if timeoutReached {
		fmt.Println("Timeout reached waiting for ES to be ready for requests")
		os.Exit(1)
	}

	// Apply mappings
	es, err := elasticsearch.NewDefaultClient()
	if err != nil {
		log.Fatalf("Error creating the client: %s", err)
	}

	res, err := es.Info()
	if err != nil {
		log.Fatalf("Error getting response: %s", err)
	}
	data, _ := ioutil.ReadAll(res.Body)
	fmt.Println(string(data))
	// // Create a new index.
	// createIndex, err := client.CreateIndex("twitter").BodyString(mapping).Do(ctx)
	// if err != nil {
	// 	// Handle error
	// 	panic(err)
	// }
	// if !createIndex.Acknowledged {
	// 	// Not acknowledged
	// }

	// Add data
}

func contains(slice []string, value string) bool {
	for _, s := range slice {
		if s == value {
			return true
		}
	}
	return false
}
