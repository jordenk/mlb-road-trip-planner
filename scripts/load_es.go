package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"
)

const esHost = "127.0.0.1:9200"
const timeoutSeconds = 2 * 60

func main() {
	startTime := time.Now().Unix()

	for (time.Now().Unix() - startTime) < timeoutSeconds {
		fmt.Println(startTime)
		resp, err := http.Get(fmt.Sprintf("http://%s/_cat/health", esHost))
		if err != nil {
			fmt.Printf("The HTTP request failed with error %s\n", err)
		} else {
			data, _ := ioutil.ReadAll(resp.Body)
			stringSlice := strings.Split(string(data), " ")
			// contains() is a little safer than a regex check
			if contains(stringSlice, "green") {
				fmt.Println("ES ready for indexing!")
				break
			}
		}
		fmt.Println("Waiting for Elasticsearch to be ready for requests...")
		time.Sleep(5 * time.Second)
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

// start_time_seconds = time.time()
// timeout = False

// while not timeout:
//     resp = requests.get(f"http://{ES_HOST}:{ES_PORT}/_cat/health")
//     if resp.status_code == 200:
//         if "green" in resp.content.decode():
//             print("Elasticsearch is ready for indexing.")
//             break
//     timeout = (time.time() - start_time_seconds) < TIMEOUT_SECONDS

// if timeout:
//     print("ERROR: Timout reached waiting for ES to be ready for indexing. Check cluster health.")
//     sys.exit(1)

// # Apply mappings

// # Add data
