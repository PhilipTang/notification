package main

import (
	notification ".."
	config "../config"

	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/go-redis/redis"
	"github.com/golang/glog"
)

var (
	brokers     = flag.String("brokers", os.Getenv("KAFKA_PEERS"), "The comma separated list of brokers in the Kafka cluster")
	topic       = flag.String("topic", "", "REQUIRED: the topic to consume")
	partitions  = flag.String("partitions", "all", "The partitions to consume, can be 'all' or comma-separated numbers")
	offset      = flag.String("offset", "newest", "The offset to start with. Can be `oldest`, `newest`")
	verbose     = flag.Bool("verbose", false, "Whether to turn on sarama logging")
	bufferSize  = flag.Int("buffer-size", 256, "The buffer size of the message channel.")
	redisClient *redis.Client
)

func init() {
	flag.Parse()

	if *brokers == "" {
		printUsageErrorAndExit("You have to provide -brokers as a comma-separated list, or set the KAFKA_PEERS environment variable.")
	}
	if *topic == "" {
		printUsageErrorAndExit("-topic is required")
	}
	if *verbose {
		sarama.Logger = log.New(os.Stderr, "listener ", log.LstdFlags)
	}

	redisClient = redis.NewClient(&redis.Options{
		Addr:     config.MyConfig.Redis.Server,   // TODO use config yaml
		Password: config.MyConfig.Redis.Password, // no password set
		DB:       config.MyConfig.Redis.DB,       // use default DB
	})
	if pong, err := redisClient.Ping().Result(); err != nil {
		printErrorAndExit(69, "connect to redis failed")
	} else {
		glog.Infof("PING redis output: %s", pong)
	}
}

func main() {
	var initialOffset int64
	switch *offset {
	case "oldest":
		initialOffset = sarama.OffsetOldest
	case "newest":
		initialOffset = sarama.OffsetNewest
	default:
		initialOffset, _ = strconv.ParseInt(*offset, 10, 64)
	}

	brokerList := strings.Split(*brokers, ",")
	c, err := sarama.NewConsumer(brokerList, nil)
	if err != nil {
		printErrorAndExit(69, "Failed to start consumer: %s", err)
	}

	partitionList, err := getPartitions(c)
	if err != nil {
		printErrorAndExit(69, "Failed to get the list of partitions: %s", err)
	}

	var (
		messages = make(chan *sarama.ConsumerMessage, *bufferSize)
		closing  = make(chan struct{})
		wg       sync.WaitGroup
	)

	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Kill, os.Interrupt)
		<-signals
		glog.Info("Initiating shutdown of consumer...")
		close(closing)
	}()

	for _, partition := range partitionList {
		pc, err := c.ConsumePartition(*topic, partition, initialOffset)
		if err != nil {
			printErrorAndExit(69, "Failed to start consumer for partition %d: %s", partition, err)
		}

		go func(pc sarama.PartitionConsumer) {
			<-closing
			pc.AsyncClose()
		}(pc)

		wg.Add(1)

		go func(pc sarama.PartitionConsumer) {
			defer wg.Done()
			for message := range pc.Messages() {
				messages <- message
			}
		}(pc)
	}

	go func() {
		for message := range messages {
			go notification.Fire(redisClient, message, "", notification.MessageRetry{})
		}
	}()

	wg.Wait()

	glog.Info("Done consuming topic", *topic)
	close(messages)

	if err := c.Close(); err != nil {
		glog.Info("Failed to close consumer: ", err)
	}
}

func getPartitions(c sarama.Consumer) ([]int32, error) {
	if *partitions == "all" {
		return c.Partitions(*topic)
	}

	tmp := strings.Split(*partitions, ",")
	var pList []int32
	for i := range tmp {
		val, err := strconv.ParseInt(tmp[i], 10, 32)
		if err != nil {
			return nil, err
		}
		pList = append(pList, int32(val))
	}

	return pList, nil
}

func printErrorAndExit(code int, format string, values ...interface{}) {
	fmt.Fprintf(os.Stderr, "ERROR: %s\n", fmt.Sprintf(format, values...))
	fmt.Fprintln(os.Stderr)
	os.Exit(code)
}

func printUsageErrorAndExit(format string, values ...interface{}) {
	fmt.Fprintf(os.Stderr, "ERROR: %s\n", fmt.Sprintf(format, values...))
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Available command line options:")
	flag.PrintDefaults()
	os.Exit(64)
}
