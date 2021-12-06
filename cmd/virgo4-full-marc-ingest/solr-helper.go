package main

import (
	"log"
	"time"
)

func ensureSOLREndpointExists(endpoint string) error {
	log.Printf("INFO: checking SOLR endpoint %s", endpoint)
	return nil
}

func deleteOldSolrRecords(endpoint string, dataSource string, olderThan time.Time) error {
	log.Printf("INFO: deleting SOLR records (%s) older than %s", dataSource, olderThan)
	return nil
}

//
// end of file
//
