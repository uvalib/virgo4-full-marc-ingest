package main

import (
	"fmt"
	"log"

	dbx "github.com/go-ozzo/ozzo-dbx"
	_ "github.com/lib/pq"
)

//dbHandle *dbx.DB

func newDBConnection(cfg *ServiceConfig) error {

	// connect to database
	log.Printf("INFO: creating postgres connection")

	connStr := fmt.Sprintf("user=%s password=%s dbname=%s host=%s port=%d connect_timeout=%d sslmode=disable",
		cfg.PostgresUser, cfg.PostgresPass, cfg.PostgresDatabase, cfg.PostgresHost, cfg.PostgresPort, 30)

	_, err := dbx.MustOpen("postgres", connStr)
	if err != nil {
		return err
	}
	return nil
}

//
// end of file
//
