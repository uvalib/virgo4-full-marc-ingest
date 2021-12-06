package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"time"
)

var maxHttpRetries = 3
var retrySleepTime = 100 * time.Millisecond

func fatalIfError(err error) {
	if err != nil {
		log.Fatalf("FATAL ERROR: %s", err.Error())
	}
}

func httpGet(httpClient *http.Client, url string) ([]byte, error) {

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	var response *http.Response
	count := 0
	for {
		response, err = httpClient.Do(req)

		count++
		if err != nil {
			if canRetry(err) == false {
				return nil, err
			}

			// break when tried too many times
			if count >= maxHttpRetries {
				return nil, err
			}

			log.Printf("WARNING: HTTP get failed with error, retrying (%s)", err)

			// sleep for a bit before retrying
			time.Sleep(retrySleepTime)
		} else {

			defer response.Body.Close()

			body, err := ioutil.ReadAll(response.Body)

			// happy day, hopefully all is well
			if response.StatusCode == http.StatusOK {

				// if the body read failed
				if err != nil {
					log.Printf("ERROR: read failed with error (%s)", err)
					return nil, err
				}

				return body, nil
			}

			log.Printf("ERROR: HTTP get failed with status %d (%s)", response.StatusCode, body)

			return body, fmt.Errorf("request returns HTTP %d", response.StatusCode)
		}
	}
}

func httpPost(httpClient *http.Client, url string, buffer []byte) ([]byte, error) {

	var response *http.Response
	count := 0

	for {
		req, err := http.NewRequest("POST", url, bytes.NewBuffer(buffer))
		if err != nil {
			return nil, err
		}

		req.Header.Set("Content-Type", "application/xml")

		response, err = httpClient.Do(req)
		count++
		if err != nil {
			if canRetry(err) == false {
				return nil, err
			}

			// break when tried too many times
			if count >= maxHttpRetries {
				return nil, err
			}

			log.Printf("WARNING: HTTP post failed with error, retrying (%s)", err)

			// sleep for a bit before retrying
			time.Sleep(retrySleepTime)
		} else {

			defer response.Body.Close()

			body, err := ioutil.ReadAll(response.Body)

			// happy day, hopefully all is well
			if response.StatusCode == http.StatusOK {

				// if the body read failed
				if err != nil {
					log.Printf("ERROR: read failed with error (%s)", err)
					return nil, err
				}

				// everything went OK
				return body, nil
			}

			log.Printf("ERROR: HTTP post failed with status %d (%s)", response.StatusCode, body)
			return body, fmt.Errorf("request returns HTTP %d", response.StatusCode)
		}
	}
}

// examines the error and decides if it can be retried
func canRetry(err error) bool {

	if strings.Contains(err.Error(), "operation timed out") == true {
		return true
	}

	if strings.Contains(err.Error(), "Client.Timeout exceeded") == true {
		return true
	}

	if strings.Contains(err.Error(), "write: broken pipe") == true {
		return true
	}

	if strings.Contains(err.Error(), "no such host") == true {
		return true
	}

	if strings.Contains(err.Error(), "network is down") == true {
		return true
	}

	return false
}

//
// end of file
//
