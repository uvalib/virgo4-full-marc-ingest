package main

import (
	"fmt"
	"github.com/uvalib/virgo4-sqs-sdk/awssqs"
	"log"
	"time"
)

var ErrQueuesNotIdle = fmt.Errorf("queues did not become idle before timeout")

func ensureQueuesExist(aws awssqs.AWS_SQS, queues []string) error {

	for _, q := range queues {
		_, err := aws.QueueHandle(q)
		if err != nil {
			log.Printf("ERROR: queue %s does not exist", q)
			return err
		}
	}
	return nil
}

func ensureQueuesIdle(aws awssqs.AWS_SQS, queues []string, polltime int, timeout int) error {

	start := time.Now()
	idleCount := 0 // we to handle in-flight too so we wait for 3 idle iterations
	for {
		// get counts for all the queues we are interested in
		counts, err := getQueueMessageCounts(aws, queues)
		if err != nil {
			return err
		}

		// all queues are idle, we can return
		if allQueuesIdle(counts) == true {
			idleCount++
			if idleCount == 3 {
				log.Printf("INFO: all queues are now idle")
				return nil
			}
		} else {
			idleCount = 0
		}

		// determine if it is time to give up
		elapsed := int64(time.Since(start) / time.Second)
		if elapsed >= int64(timeout) {
			log.Printf("ERROR: queues not idle after %d seconds, giving up", timeout)
			return ErrQueuesNotIdle
		}

		// not time to give up, just wait for a while
		log.Printf("INFO: queues not yet idle...")
		time.Sleep(time.Duration(polltime) * time.Second)
	}
}

func getQueueMessageCounts(aws awssqs.AWS_SQS, queues []string) ([]uint, error) {

	counts := make([]uint, len(queues))
	for ix, q := range queues {
		count, err := getQueueMessageCount(aws, q)
		if err != nil {
			return counts, err
		}
		counts[ix] = count
	}
	return counts, nil
}

func getQueueMessageCount(aws awssqs.AWS_SQS, queue string) (uint, error) {

	count, err := aws.GetMessagesAvailable(queue)
	if err != nil {
		return 0, err
	}
	if count > 0 {
		log.Printf("INFO: queue %s still contains %d items", queue, count)
	}
	return count, nil
}

func allQueuesIdle(counts []uint) bool {
	for _, c := range counts {
		if c != 0 {
			return false
		}
	}
	return true
}

//
// end of file
//
