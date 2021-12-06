package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/uvalib/virgo4-sqs-sdk/awssqs"
)

var ErrTooManyUnprocessedItems = fmt.Errorf("too many unprocessed items")

//
// main entry point
//
func main() {

	log.Printf("===> %s service staring up (version: %s) <===", os.Args[0], Version())

	// Get config params and use them to init service context. Any issues are fatal
	cfg := LoadConfiguration()

	// establish the database connection
	err := newDBConnection(cfg)
	fatalIfError(err)

	// load our AWS_SQS helper object
	aws, err := awssqs.NewAwsSqs(awssqs.AwsSqsConfig{MessageBucketName: cfg.MessageBucketName})
	fatalIfError(err)

	// ensure the queues exist
	fatalIfError(ensureQueuesExist(aws, append(cfg.WaitIdleQueues, cfg.ErrorQueue)))

	// get the queue handles from the queue name
	inQueueHandle, err := aws.QueueHandle(cfg.InQueueName)
	fatalIfError(err)

	outQueueHandle, err := aws.QueueHandle(cfg.OutQueueName)
	fatalIfError(err)

	var cacheQueueHandle awssqs.QueueHandle
	if cfg.CacheQueueName != "" {
		cacheQueueHandle, err = aws.QueueHandle(cfg.CacheQueueName)
		fatalIfError(err)
	}

	// create the record channel
	marcRecordsChan := make(chan Record, cfg.WorkerQueueSize)

	// start workers here
	for w := 1; w <= cfg.Workers; w++ {
		go worker(w, *cfg, aws, outQueueHandle, cacheQueueHandle, marcRecordsChan)
	}

	for {
		// top of our processing loop
		err = nil

		// notification that there is one or more new ingest files to be processed
		inbound, receiptHandle, e := getInboundNotification(*cfg, aws, inQueueHandle)
		fatalIfError(e)

		// download each file and validate it
		localNames := make([]string, 0, len(inbound))
		for ix, f := range inbound {

			// download the file
			localFile, e := s3download(cfg.DownloadDir, f.SourceBucket, f.SourceKey, f.ObjectSize)
			fatalIfError(e)

			// save the local name, we will need it later
			localNames = append(localNames, localFile)

			log.Printf("INFO: validating %s/%s (%s)", f.SourceBucket, f.SourceKey, localNames[ix])

			// create a new loader
			loader, e := NewRecordLoader(f.SourceKey, localNames[ix])
			fatalIfError(e)

			// validate the file
			e = loader.Validate()
			loader.Done()
			if e == nil {
				log.Printf("INFO: %s/%s (%s) appears to be OK, ready for ingest", f.SourceBucket, f.SourceKey, localNames[ix])
			} else {
				log.Printf("ERROR: %s/%s (%s) appears to be invalid, ignoring it (%s)", f.SourceBucket, f.SourceKey, localNames[ix], e.Error())
				err = e
				break
			}
		}

		// one of the files was invalid, we need to ignore the entire batch and delete the local files
		if err != nil {
			for _, f := range localNames {
				log.Printf("INFO: removing invalid file %s", f)
				err = os.Remove(f)
				fatalIfError(err)
			}

			// go back to waiting for the next notification
			continue
		}

		// if we got here without an error then all the files are valid to be loaded... we can delete the inbound message
		// because it has been processed

		delMessages := make([]awssqs.Message, 0, 1)
		delMessages = append(delMessages, awssqs.Message{ReceiptHandle: receiptHandle})
		opStatus, err := aws.BatchMessageDelete(inQueueHandle, delMessages)
		if err != nil {
			if err != awssqs.ErrOneOrMoreOperationsUnsuccessful {
				fatalIfError(err)
			}
		}

		// check the operation results
		for ix, op := range opStatus {
			if op == false {
				log.Printf("ERROR: message %d failed to delete", ix)
			}
		}

		//
		// the inbound file(s) have been downloaded and validated, we need to do the other pre-processing steps now
		//

		// disable the ingest services
		err = stopManagedServices(cfg.ECSClusterName, cfg.ManagedECSServices)
		fatalIfError(err)

		// wait until the work queues are idle
		err = ensureQueuesIdle(aws, cfg.WaitIdleQueues, int(cfg.PollTimeOut), cfg.WaitForIdleStart)
		fatalIfError(err)

		// once processing is complete, we will delete old records so we need to capture the time we start
		//startIngest := time.Now()
		startIngest := time.Date(2018, 0, 1, 0, 0, 0, 0, time.UTC)

		// now we can process each of the inbound files
		for ix, f := range inbound {

			start := time.Now()
			log.Printf("INFO: processing %s/%s (%s)", f.SourceBucket, f.SourceKey, localNames[ix])

			loader, err := NewRecordLoader(f.SourceKey, localNames[ix])
			// fatal fail here because we have already validated the file and believe it to be correct so this
			// is some other sort of failure
			fatalIfError(err)

			// get the first record
			count := 0
			rec, err := loader.First(true)
			if err != nil {
				// are we done
				if err == io.EOF {
					log.Printf("WARNING: EOF on first read, looks like an empty file")
				} else {
					// fatal fail here because we have already validated the file and believe it to be correct so this
					// is some other sort of failure
					log.Fatal(err)
				}
			}

			// we can get here with an error if the first read yields EOF
			if err == nil {
				for {

					// here we overwrite the record source if configured to do so, otherwise we use the
					// one from the loader, determined by the filename.

					if cfg.DataSource != "" {
						rec.SetSource(cfg.DataSource)
					}

					count++
					marcRecordsChan <- rec

					rec, err = loader.Next(true)
					if err != nil {
						if err == io.EOF {
							// this is expected, break out of the processing loop
							break
						}
						// fatal fail here because we have already validated the file and believe it to be correct so this
						// is some other sort of failure
						log.Fatal(err)
					}
				}
			}

			loader.Done()
			duration := time.Since(start)
			log.Printf("INFO: done processing %s/%s (%s). %d records (%0.2f tps)", f.SourceBucket, f.SourceKey, localNames[ix], count, float64(count)/duration.Seconds())

			// file has been ingested, remove it
			log.Printf("INFO: removing processed file %s", localNames[ix])
			err = os.Remove(localNames[ix])
			fatalIfError(err)
		}

		// wait until we have processed all outbound items
		for {
			pending := len(marcRecordsChan)
			// is our queue empty
			if pending == 0 {
				// wait until the workers have flushed their queues
				time.Sleep(flushTimeout)
				break
			} else {
				log.Printf("INFO: waiting for all records to be queued (%d remain)", pending)
				time.Sleep(flushTimeout)
			}
		}

		// wait until the work queues are idle
		err = ensureQueuesIdle(aws, cfg.WaitIdleQueues, int(cfg.PollTimeOut), cfg.WaitForIdleEnd)
		fatalIfError(err)

		// delete old SOLR stuff
		err = deleteOldSolrRecords(cfg.DataSource, cfg.SOLRMaster, startIngest)
		fatalIfError(err)

		// delete old cache stuff
		err = deleteOldCacheRecords(cfg.DataSource, startIngest)
		fatalIfError(err)

		// determine of we have unprocessed items and abort if we have too many
		unprocessed, err := getQueueMessageCount(aws, cfg.ErrorQueue)
		fatalIfError(err)
		if unprocessed >= uint(cfg.ErrorThreshold) {
			log.Printf("ERROR: too many unprocessed items (%d)", unprocessed)
			fatalIfError(ErrTooManyUnprocessedItems)
		}

		// re-enable the ingest services
		err = startManagedServices(cfg.ECSClusterName, cfg.ManagedECSServices)
		fatalIfError(err)
	}
}

//
// end of file
//
