// 消息通知重试程序
// @link https://github.com/YunzhanghuOpen/notification/issues/4
package main

import (
	notification ".."
	config "../config"

	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/go-redis/redis"
	"github.com/golang/glog"
)

var (
	brokers     = flag.String("brokers", os.Getenv("KAFKA_PEERS"), "The comma separated list of brokers in the Kafka cluster")
	topic       = flag.String("topic", "", "REQUIRED: the topic to consume")
	verbose     = flag.Bool("verbose", false, "Whether to turn on sarama logging")
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
		sarama.Logger = log.New(os.Stderr, "listener-retry ", log.LstdFlags)
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
	fn := "main"

	var lists []string
	lists = append(lists, fmt.Sprintf(notification.FORMAT_LIST, *topic, 2, "4m"))
	lists = append(lists, fmt.Sprintf(notification.FORMAT_LIST, *topic, 3, "10m"))
	lists = append(lists, fmt.Sprintf(notification.FORMAT_LIST, *topic, 4, "10m"))
	lists = append(lists, fmt.Sprintf(notification.FORMAT_LIST, *topic, 5, "1h"))
	lists = append(lists, fmt.Sprintf(notification.FORMAT_LIST, *topic, 6, "2h"))
	lists = append(lists, fmt.Sprintf(notification.FORMAT_LIST, *topic, 7, "6h"))
	lists = append(lists, fmt.Sprintf(notification.FORMAT_LIST, *topic, 8, "15h"))
	listsCount := len(lists)
	listsLastIndex := listsCount - 1

	for {
		for i, listKey := range lists {
			glog.V(10).Infof("@%s, list=%s", fn, listKey)
			for {
				var (
					listRes []string
					err     error
					offset  int64
				)
				listRes, err = redisClient.LRange(listKey, 0, 0).Result()
				if err != nil {
					glog.Errorf("@%s, redisClient.LRange failed, err=%s", fn, err)
					break
				}
				if len(listRes) == 0 {
					glog.V(10).Infof("@%s, len(listRes) == 0, list=%s", fn, listKey)
					break
				}

				offset, err = strconv.ParseInt(listRes[0], 10, 64)
				if err != nil {
					glog.Errorf("@%s, strconv.ParseInt failed, err=%s, listRes=%s", fn, err, listRes[0])
					break
				}
				offsetKey := fmt.Sprintf(notification.FORMAT_HASH, *topic, offset)

				var (
					attempts  int32
					nextTime  int64
					partition int32
				)
				if attempts, nextTime, partition, err = getRetryData(redisClient, offsetKey); err != nil {
					glog.Errorf("@%s, getRetryData failed, err=%s, offsetKey=%s", fn, err, offsetKey)
					break
				}

				if nextTime > time.Now().Unix() {
					break
				}

				var dest string
				if i < listsLastIndex {
					dest = lists[i+1]
				} else {
					dest = ""
				}

				go retry(offset, partition, dest, notification.MessageRetry{
					Offset:    offset,
					Partition: partition,
					Attempts:  attempts,
					NextTime:  nextTime,
				})

				if popOffset, err := redisClient.LPop(listKey).Result(); err != nil {
					glog.Errorf("@%s, redisClient.LPop failed, err=%s, key=%s", fn, err, listKey)
					break
				} else {
					if popOffset != listRes[0] {
						glog.Errorf("@%s, popOffset != listRes[0], key=%s, popOffset=%s, listRes=%+v", fn, listKey, popOffset, listRes)
						break
					}
				}
			}
		}
		time.Sleep(time.Minute * 1)
	}
}

func getRetryData(redisClient *redis.Client, offsetKey string) (attempts int32, nextTime int64, partition int32, err error) {
	fn := "getRetryData"

	var (
		strAttempts  string
		strNextTime  string
		strPartition string
	)
	strAttempts, err = redisClient.HGet(offsetKey, "attempts").Result()
	if err != nil {
		glog.Errorf("@%s, redisClient.HGet failed, field=%s, key=%s", fn, err, "attempts", offsetKey)
		return
	}
	strNextTime, err = redisClient.HGet(offsetKey, "next_time").Result()
	if err != nil {
		glog.Errorf("@%s, redisClient.HGet failed, field=%s, key=%s", fn, err, "next_time", offsetKey)
		return
	}
	strPartition, err = redisClient.HGet(offsetKey, "partition").Result()
	if err != nil {
		glog.Errorf("@%s, redisClient.HGet failed, field=%s, key=%s", fn, err, "partition", offsetKey)
		return
	}

	var tmp int64
	if tmp, err = strconv.ParseInt(strAttempts, 10, 32); err != nil {
		glog.Errorf("@%s, strconv.ParseInt failed, err=%s, s=%s", fn, err, strAttempts)
		return
	} else {
		attempts = int32(tmp)
	}
	if tmp, err = strconv.ParseInt(strNextTime, 10, 32); err != nil {
		glog.Errorf("@%s, strconv.ParseInt failed, err=%s, s=%s", fn, err, strNextTime)
		return
	} else {
		nextTime = tmp
	}
	if tmp, err = strconv.ParseInt(strPartition, 10, 32); err != nil {
		glog.Errorf("@%s, strconv.ParseInt failed, err=%s, s=%s", fn, err, strPartition)
		return
	} else {
		partition = int32(tmp)
	}

	return
}

func retry(offset int64, partition int32, dest string, retryData notification.MessageRetry) (err error) {
	fn := "retry"

	brokerList := strings.Split(*brokers, ",")
	consumer, err := sarama.NewConsumer(brokerList, nil)
	if err != nil {
		glog.Errorf("@%s, sarama.NewConsumer failed, err=%s, brokerList=%+v", fn, err, brokerList)
		return
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			glog.Errorf("@%s, Failed to close consumer, err=%s", fn, err)
			return
		}
	}()

	var pc sarama.PartitionConsumer
	pc, err = consumer.ConsumePartition(*topic, partition, offset)
	if err != nil {
		glog.Errorf("@%s, consumer.ConsumePartition failed, err=%s, topic=%s, partition=%d, offset=%d", fn, err, *topic, partition, offset)
		return
	}

	var (
		message      *sarama.ConsumerMessage
		messageValid bool = false
	)

	select {
	case val := <-pc.Messages():
		message = val
		messageValid = true
	}
	pc.AsyncClose()

	if messageValid == false {
		err = errors.New(fmt.Sprintf("@%s, <-pc.Messages() failed, messageValid == false, offset=%d, partition=%d", fn, offset, partition))
		glog.Error(err)
		return
	}

	if err = notification.Fire(redisClient, message, dest, retryData); err != nil {
		glog.Errorf("@%s, notification.Fire failed, err=%s, message=%+v, dest=%s", fn, err, message, dest)
		return
	}

	return
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
