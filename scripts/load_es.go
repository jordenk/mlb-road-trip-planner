package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"log"
	"os"

	"github.com/elastic/go-elasticsearch/esapi"

	"github.com/elastic/go-elasticsearch"
)

const esHost = "localhost:9200"
const indexName = "games"
const timeoutSeconds = 2 * 60

func main() {
	// Wait for cluster to be ready
	// startTime := time.Now().Unix()
	// timeoutReached := false

	// for !timeoutReached {
	// 	res, err := http.Get(fmt.Sprintf("http://%s/_cat/health", esHost))
	// 	if err != nil {
	// 		fmt.Printf("The HTTP request failed with error %s\n", err)
	// 	} else {
	// 		data, _ := ioutil.ReadAll(res.Body)
	// 		stringSlice := strings.Split(string(data), " ")
	// 		// contains() is a little safer than a regex check
	// 		if contains(stringSlice, "green") || contains(stringSlice, "yellow") {
	// 			fmt.Println("ES ready for indexing!")
	// 			break
	// 		}
	// 	}
	// 	fmt.Println("Waiting for Elasticsearch to be ready for requests...")
	// 	time.Sleep(5 * time.Second)
	// 	timeoutReached = (time.Now().Unix() - startTime) > timeoutSeconds
	// }

	// if timeoutReached {
	// 	log.Fatalf("Timeout reached waiting for ES to be ready for requests")
	// }

	// Apply mappings
	// Use the http client to create the index because the es client was throwing errors.
	workingDir, _ := os.Getwd()
	// gamesMappingsFile, err := os.Open(workingDir + "/data/games-mappings.json")
	// if err != nil {
	// 	log.Fatalf("Error reading mappings: %s", err)
	// }

	// defer gamesMappingsFile.Close()

	// client := &http.Client{}
	// esPutURL := fmt.Sprintf("http://%s/%s", esHost, indexName)
	// req, err := http.NewRequest(http.MethodPut, esPutURL, gamesMappingsFile)
	// req.Header.Set("content-type", "application/json; charset=UTF-8")
	// if err != nil {
	// 	log.Fatalf("Error building request: %s", err)
	// }
	// resp, err := client.Do(req)
	// if err != nil {
	// 	log.Fatalf("Error indexing ES with PUT request: %s", err)
	// }

	// respBodyStr, _ := ioutil.ReadAll(resp.Body)
	// fmt.Printf("Successfully created ES index. ES response:\n%s", respBodyStr)

	// Bulk index data into Elasticsearch
	gameDataFile, err := os.Open(workingDir + "/data/mlb-games.jsonl")
	if err != nil {
		log.Fatalf("Error reading data file: %s", err)
	}

	defer gameDataFile.Close()

	esClient, err := elasticsearch.NewDefaultClient()
	if err != nil {
		log.Fatalf("Error building ES client: %s", err)
	}

	fileReader := bufio.NewReader(gameDataFile)
	esDataReader := bytes.NewReader([]byte{})
	count := 0
	for {
		bytesLine, prefix, _ := fileReader.ReadLine()
		count++
		// break at file end
		if len(bytesLine) == 0 && !prefix {
			resp, err := esapi.BulkRequest{
				Index: indexName,
				Body:  esDataReader,
			}.Do(context.Background(), esClient)
			fmt.Println(resp)

			fmt.Println(err)
			break
		}
		// err check?
		i, _ := esDataReader.Read(bytesLine)
		fmt.Println(i)
		if count%500 == 0 {
			resp, err := esapi.BulkRequest{
				Index: indexName,
				Body:  esDataReader,
			}.Do(context.Background(), esClient)

			esDataReader = bytes.NewReader([]byte{})

			fmt.Println(resp)

			fmt.Println(err)
		}
	}

}

func contains(slice []string, value string) bool {
	for _, s := range slice {
		if s == value {
			return true
		}
	}
	return false
}
