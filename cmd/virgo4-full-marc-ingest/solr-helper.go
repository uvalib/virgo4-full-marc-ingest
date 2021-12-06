package main

import (
	"fmt"
	"log"
	"net/http"
	"time"
)

var deletePayload = "<delete><query>timestamp:[* TO \"{:before}\"] AND data_source_f:{:datasource}</query></delete>"
var httpClient *http.Client

func ensureSOLREndpointExists(endpoint string, core string, timeout int) error {
	log.Printf("INFO: checking SOLR endpoint %s", endpoint)

	// configure the client
	httpClient := &http.Client{
		Timeout: time.Duration(timeout) * time.Second,
	}

	pingUrl := fmt.Sprintf("%s/%s/admin/ping", endpoint, core)
	_, err := httpGet(httpClient, pingUrl)
	return err
}

func deleteOldSolrRecords(endpoint string, core string, timeout int, dataSource string, olderThan time.Time) error {
	log.Printf("INFO: deleting SOLR records (%s) older than %s", dataSource, olderThan)
	return nil
}

//
// end of file
//
