package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

const esHost = "localhost:9200"
const indexName = "games"
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
			if contains(stringSlice, "green") || contains(stringSlice, "yellow") {
				fmt.Println("ES ready for indexing!")
				break
			}
		}
		fmt.Println("Waiting for Elasticsearch to be ready for requests...")
		time.Sleep(5 * time.Second)
		timeoutReached = (time.Now().Unix() - startTime) > timeoutSeconds
	}

	if timeoutReached {
		log.Fatalf("Timeout reached waiting for ES to be ready for requests")
	}

	// Apply mappings
	// Use the http client to create the index because the es client was throwing errors.
	workingDir, _ := os.Getwd()
	jsonFile, err := os.Open(workingDir + "/data/games-mappings.json")
	if err != nil {
		log.Fatalf("Error reading mappings: %s", err)
	}

	defer jsonFile.Close()

	client := &http.Client{}
	esPutURL := fmt.Sprintf("http://%s/%s", esHost, indexName)
	req, err := http.NewRequest(http.MethodPut, esPutURL, jsonFile)
	req.Header.Set("content-type", "application/json; charset=UTF-8")
	if err != nil {
		log.Fatalf("Error building request: %s", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatalf("Error putting request: %s", err)
	}
	str, _ := ioutil.ReadAll(resp.Body)
	log.Println(string(str))
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
