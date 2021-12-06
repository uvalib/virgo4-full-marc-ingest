package main

import (
	"log"
	"os"
	"strconv"
	"strings"
)

// ServiceConfig defines all of the service configuration parameters
type ServiceConfig struct {
	InQueueName    string // SQS queue name for inbound notifications
	OutQueueName   string // SQS queue name for outbound documents
	CacheQueueName string // SQS queue name for cache documents (typically records go to the cache)
	PollTimeOut    int64  // the SQS queue timeout (in seconds)

	DataSource        string // the name to associate the data with. Each record has metadata showing this value
	MessageBucketName string // the bucket to use for large messages
	DownloadDir       string // the S3 file download directory (local)

	WorkerQueueSize int // the inbound message queue size to feed the workers
	Workers         int // the number of worker processes

	WaitIdleQueues   []string // list of queues we need to wait on to determine we are idle
	ErrorQueue       string   // queue to determine if there were errors during processing
	ErrorThreshold   int      // the number of errors before we abandon the processing sequence
	WaitForIdleStart int      // the time to wait for idle at the start of processing
	WaitForIdleEnd   int      // the time to wait for idle at the end of processing

	ECSClusterName     string   // the cluster name containing the managed services
	ManagedECSServices []string // list of services to manage during processing (stop at the beginning and restart at the end)

	SolrMaster  string // SOLR master endpoint
	SolrCore    string // SOLR core name
	SolrTimeout int    // SOLR communication timeout

	PostgresHost     string // database endpoint name
	PostgresPort     int    // database port
	PostgresUser     string // database user
	PostgresPass     string // database password
	PostgresDatabase string // database name
}

func envWithDefault(env string, defaultValue string) string {
	val, set := os.LookupEnv(env)

	if set == false {
		log.Printf("environment variable not set: [%s] using default value [%s]", env, defaultValue)
		return defaultValue
	}

	return val
}

func ensureSet(env string) string {
	val, set := os.LookupEnv(env)

	if set == false {
		log.Printf("environment variable not set: [%s]", env)
		os.Exit(1)
	}

	return val
}

func ensureSetAndNonEmpty(env string) string {
	val := ensureSet(env)

	if val == "" {
		log.Printf("environment variable not set: [%s]", env)
		os.Exit(1)
	}

	return val
}

func envToInt(env string) int {

	number := ensureSetAndNonEmpty(env)
	n, err := strconv.Atoi(number)
	fatalIfError(err)
	return n
}

func splitMultiple(env string) []string {
	return strings.Split(env, " ")
}

// LoadConfiguration will load the service configuration from env/cmdline
// and return a pointer to it. Any failures are fatal.
func LoadConfiguration() *ServiceConfig {

	var cfg ServiceConfig

	cfg.InQueueName = ensureSetAndNonEmpty("VIRGO4_FULL_MARC_INGEST_IN_QUEUE")
	cfg.OutQueueName = ensureSetAndNonEmpty("VIRGO4_FULL_MARC_INGEST_OUT_QUEUE")
	cfg.CacheQueueName = envWithDefault("VIRGO4_FULL_MARC_INGEST_CACHE_QUEUE", "")
	cfg.PollTimeOut = int64(envToInt("VIRGO4_FULL_MARC_INGEST_QUEUE_POLL_TIMEOUT"))
	cfg.DataSource = envWithDefault("VIRGO4_FULL_MARC_INGEST_DATA_SOURCE", "")
	cfg.MessageBucketName = ensureSetAndNonEmpty("VIRGO4_SQS_MESSAGE_BUCKET")
	cfg.DownloadDir = ensureSetAndNonEmpty("VIRGO4_FULL_MARC_INGEST_DOWNLOAD_DIR")
	cfg.WorkerQueueSize = envToInt("VIRGO4_FULL_MARC_INGEST_WORK_QUEUE_SIZE")
	cfg.Workers = envToInt("VIRGO4_FULL_MARC_INGEST_WORKERS")

	cfg.WaitIdleQueues = splitMultiple(ensureSetAndNonEmpty("VIRGO4_FULL_MARC_INGEST_IDLE_QUEUES"))
	cfg.WaitForIdleStart = envToInt("VIRGO4_FULL_MARC_INGEST_START_IDLE_WAIT")
	cfg.WaitForIdleEnd = envToInt("VIRGO4_FULL_MARC_INGEST_END_IDLE_WAIT")
	cfg.ErrorQueue = ensureSetAndNonEmpty("VIRGO4_FULL_MARC_INGEST_ERROR_QUEUE")
	cfg.ErrorThreshold = envToInt("VIRGO4_FULL_MARC_INGEST_ERROR_THRESHOLD")
	cfg.ECSClusterName = ensureSetAndNonEmpty("VIRGO4_FULL_MARC_INGEST_CLUSTER_NAME")
	cfg.ManagedECSServices = splitMultiple(ensureSetAndNonEmpty("VIRGO4_FULL_MARC_INGEST_MANAGED_SERVICES"))
	cfg.SolrMaster = ensureSetAndNonEmpty("VIRGO4_FULL_MARC_INGEST_SOLR_MASTER")
	cfg.SolrCore = ensureSetAndNonEmpty("VIRGO4_FULL_MARC_INGEST_SOLR_CORE")
	cfg.SolrTimeout = envToInt("VIRGO4_FULL_MARC_INGEST_SOLR_TIMEOUT")

	cfg.PostgresHost = ensureSetAndNonEmpty("VIRGO4_FULL_MARC_INGEST_POSTGRES_HOST")
	cfg.PostgresPort = envToInt("VIRGO4_FULL_MARC_INGEST_POSTGRES_PORT")
	cfg.PostgresUser = ensureSetAndNonEmpty("VIRGO4_FULL_MARC_INGEST_POSTGRES_USER")
	cfg.PostgresPass = ensureSetAndNonEmpty("VIRGO4_FULL_MARC_INGEST_POSTGRES_PASS")
	cfg.PostgresDatabase = ensureSetAndNonEmpty("VIRGO4_FULL_MARC_INGEST_POSTGRES_DATABASE")

	log.Printf("[CONFIG] InQueueName          = [%s]", cfg.InQueueName)
	log.Printf("[CONFIG] OutQueueName         = [%s]", cfg.OutQueueName)
	log.Printf("[CONFIG] CacheQueueName       = [%s]", cfg.CacheQueueName)
	log.Printf("[CONFIG] PollTimeOut          = [%d]", cfg.PollTimeOut)
	log.Printf("[CONFIG] DataSource           = [%s]", cfg.DataSource)
	log.Printf("[CONFIG] MessageBucketName    = [%s]", cfg.MessageBucketName)
	log.Printf("[CONFIG] DownloadDir          = [%s]", cfg.DownloadDir)
	log.Printf("[CONFIG] WorkerQueueSize      = [%d]", cfg.WorkerQueueSize)
	log.Printf("[CONFIG] Workers              = [%d]", cfg.Workers)

	log.Printf("[CONFIG] WaitIdleQueues       = [%s]", ensureSetAndNonEmpty("VIRGO4_FULL_MARC_INGEST_IDLE_QUEUES"))
	log.Printf("[CONFIG] WaitForIdleStart     = [%d]", cfg.WaitForIdleStart)
	log.Printf("[CONFIG] WaitForIdleEnd       = [%d]", cfg.WaitForIdleEnd)
	log.Printf("[CONFIG] ErrorQueue           = [%s]", cfg.ErrorQueue)
	log.Printf("[CONFIG] ErrorThreshold       = [%d]", cfg.ErrorThreshold)
	log.Printf("[CONFIG] ECSClusterName       = [%s]", cfg.ECSClusterName)
	log.Printf("[CONFIG] ManagedECSServices   = [%s]", ensureSetAndNonEmpty("VIRGO4_FULL_MARC_INGEST_MANAGED_SERVICES"))
	log.Printf("[CONFIG] SolrMaster           = [%s]", cfg.SolrMaster)
	log.Printf("[CONFIG] SolrCore             = [%s]", cfg.SolrCore)
	log.Printf("[CONFIG] SolrTimeout          = [%d]", cfg.SolrTimeout)

	log.Printf("[CONFIG] PostgresHost         = [%s]", cfg.PostgresHost)
	log.Printf("[CONFIG] PostgresPort         = [%d]", cfg.PostgresPort)
	log.Printf("[CONFIG] PostgresUser         = [%s]", cfg.PostgresUser)
	log.Printf("[CONFIG] PostgresPass         = [REDACTED]")
	log.Printf("[CONFIG] PostgresDatabase     = [%s]", cfg.PostgresDatabase)

	// ensure the services and SOLR endpoints exist
	fatalIfError(ensureServicesExist(cfg.ECSClusterName, cfg.ManagedECSServices))
	fatalIfError(ensureSOLREndpointExists(cfg.SolrMaster, cfg.SolrCore, cfg.SolrTimeout))

	if cfg.CacheQueueName == "" {
		log.Printf("INFO: cache queue name is blank, record caching is DISABLED!!")
	}

	if cfg.DataSource == "" {
		log.Printf("INFO: data source name is blank, data source will be determined dynamically")
	}

	return &cfg
}

//
// end of file
//
