package main

import (
	"bufio"
	"bytes"
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
			log.Printf("The HTTP request failed with error %s\n", err)
		} else {
			data, _ := ioutil.ReadAll(res.Body)
			stringSlice := strings.Split(string(data), " ")
			// contains() is a little safer than a regex check
			if contains(stringSlice, "green") || contains(stringSlice, "yellow") {
				log.Println("ES ready for indexing!")
				break
			}
		}
		log.Printf("Waiting for Elasticsearch to be ready for requests...")
		time.Sleep(5 * time.Second)
		timeoutReached = (time.Now().Unix() - startTime) > timeoutSeconds
	}

	if timeoutReached {
		log.Fatalf("Timeout reached waiting for ES to be ready for requests")
	}

	// Apply mappings
	// Use the http client to create the index because the es client was throwing errors.
	workingDir, _ := os.Getwd()
	gamesMappingsFile, err := os.Open(workingDir + "/data/games-mappings.json")
	if err != nil {
		log.Fatalf("Error reading mappings: %s", err)
	}

	defer gamesMappingsFile.Close()

	client := &http.Client{}
	esPutURL := fmt.Sprintf("http://%s/%s", esHost, indexName)
	req, err := http.NewRequest(http.MethodPut, esPutURL, gamesMappingsFile)
	req.Header.Set("content-type", "application/json; charset=UTF-8")
	if err != nil {
		log.Fatalf("Error building request: %s", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatalf("Error indexing ES with PUT request: %s", err)
	}

	respBodyStr, _ := ioutil.ReadAll(resp.Body)
	log.Printf("Successfully created ES index. ES response:\n%s", respBodyStr)

	// Bulk index data into Elasticsearch
	gameDataFile, err := os.Open(workingDir + "/data/mlb-games.jsonl")
	if err != nil {
		log.Fatalf("Error reading data file: %s", err)
	}

	defer gameDataFile.Close()

	// May need to switch scanner to reader if there is a lot of data
	fileScanner := bufio.NewScanner(gameDataFile)
	byteSlice := []byte{}
	recordCount := 0

	bulkMeta := []byte(`{ "index" : { "_index" : "games", "_type" : "_doc"} }`)
	esBulkURL := fmt.Sprintf("http://%s/_bulk", esHost)

	for fileScanner.Scan() {
		// POST _bulk
		byteSlice = append(byteSlice, bulkMeta...)
		byteSlice = append(byteSlice, []byte("\n")...)
		byteSlice = append(byteSlice, fileScanner.Bytes()...)
		byteSlice = append(byteSlice, []byte("\n")...)
		recordCount++

		if recordCount%250 == 0 {
			// This is super fast. Maybe make it concurrent if we have a lot of records.
			_, err := client.Post(esBulkURL, "application/json", bytes.NewReader(byteSlice))

			if err != nil {
				log.Fatalf("Error bulk indexing ES: %s", err)
			}
			byteSlice = []byte{}
		}
	}
	// This could get pulled into a function
	_, err = client.Post(esBulkURL, "application/json", bytes.NewReader(byteSlice))

	if err != nil {
		log.Fatalf("Error bulk indexing ES: %s", err)
	}

	log.Printf("Successfully index %d records", recordCount)
}

func contains(slice []string, value string) bool {
	for _, s := range slice {
		if s == value {
			return true
		}
	}
	return false
}
