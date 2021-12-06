package main

import "log"

func ensureSOLREndpointsExist(endpoints []string) error {
	return nil
}

func stopSOLRReplication(endpoints []string) error {
	for _, e := range endpoints {
		err := disableSolrReplication(e)
		if err != nil {
			return err
		}
	}
	return nil
}

func startSOLRReplication(endpoints []string) error {
	for _, e := range endpoints {
		err := enableSolrReplication(e)
		if err != nil {
			return err
		}
	}
	return nil
}

func disableSolrReplication(endpoint string) error {
	log.Printf("INFO: disabling replication for %s", endpoint)
	return nil
}

func enableSolrReplication(endpoint string) error {
	log.Printf("INFO: enabling replication for %s", endpoint)
	return nil
}

//
// end of file
//
