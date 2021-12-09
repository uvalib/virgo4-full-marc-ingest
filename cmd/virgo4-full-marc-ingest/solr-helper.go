package main

import (
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"
)

var deletePayloadTemplate = "<delete><query>timestamp:[* TO \"{:before}\"] AND data_source_f:{:datasource}</query></delete>"

func ensureSOLREndpointExists(endpoint string, core string, timeout int) error {
	log.Printf("INFO: checking SOLR endpoint %s", endpoint)

	// configure the client
	httpClient := &http.Client{
		Timeout: time.Duration(timeout) * time.Second,
	}

	pingUrl := fmt.Sprintf("%s/%s/admin/ping", endpoint, core)
	log.Printf("INFO: URL %s", pingUrl)
	_, err := httpGet(httpClient, pingUrl)
	return err
}

func deleteOldSolrRecords(endpoint string, core string, timeout int, dataSource string, olderThan time.Time) error {
	log.Printf("INFO: deleting SOLR records (%s) older than %s", dataSource, olderThan.UTC())

	//
	// note that the timestamp field in solr is stored in the following format
	//
	// YYYY-MM-DDTHH:MM:SSZ and should be in UTC. e.g 2019-01-01T00:00:00Z
	//
	older := olderThan.UTC().Format(time.RFC3339)

	// configure the client
	httpClient := &http.Client{
		Timeout: time.Duration(timeout) * time.Second,
	}

	deleteUrl := fmt.Sprintf("%s/%s/update", endpoint, core)
	log.Printf("INFO: URL %s", deleteUrl)
	deletePayload := deletePayloadTemplate
	deletePayload = strings.ReplaceAll(deletePayload, "{:before}", older)
	deletePayload = strings.ReplaceAll(deletePayload, "{:datasource}", dataSource)
	log.Printf("INFO: Payload %s", deletePayload)
	start := time.Now()
	_, err := httpPost(httpClient, deleteUrl, []byte(deletePayload))
	if err != nil {
		log.Printf("ERROR: deleting SOLR records (%s)", err.Error())
		return err
	}
	duration := time.Since(start)
	log.Printf("INFO: SOLR delete done in %0.2f seconds", duration.Seconds())
	return nil
}

//
// end of file
//
