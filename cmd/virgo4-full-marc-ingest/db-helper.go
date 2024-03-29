package main

import (
	"fmt"
	"log"
	"time"

	dbx "github.com/go-ozzo/ozzo-dbx"
	_ "github.com/lib/pq"
)

const cacheDeleteQuery = "DELETE FROM source_cache WHERE source = {:source} AND updated_at < {:before}"

var dbHandle *dbx.DB

func newDBConnection(cfg *ServiceConfig) error {

	// connect to database
	log.Printf("INFO: creating postgres connection")

	connStr := fmt.Sprintf("user=%s password=%s dbname=%s host=%s port=%d connect_timeout=%d",
		cfg.PostgresUser, cfg.PostgresPass, cfg.PostgresDatabase, cfg.PostgresHost, cfg.PostgresPort, 30)

	var err error
	dbHandle, err = dbx.MustOpen("postgres", connStr)
	if err != nil {
		return err
	}
	return nil
}

func deleteOldCacheRecords(dataSource string, olderThan time.Time) error {
	log.Printf("INFO: deleting cache records (%s) older than %s", dataSource, olderThan.UTC())

	//
	// note that the updated_at field in the database is stored in UTC so the time does not need to be localized
	//

	q := dbHandle.NewQuery(cacheDeleteQuery)
	q.Bind(dbx.Params{"source": dataSource})
	q.Bind(dbx.Params{"before": olderThan})
	start := time.Now()
	_, err := q.Execute()
	if err != nil {
		log.Printf("ERROR: deleting old cache records (%s)", err.Error())
		return err
	}
	duration := time.Since(start)
	log.Printf("INFO: cache delete done in %0.2f seconds", duration.Seconds())

	return nil
}

//
// end of file
//
