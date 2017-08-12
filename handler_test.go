package notification

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
)

func TestPushMessage(t *testing.T) {
	meta := &MessageMeta{
		Url:         "http://123.57.137.41:90/printall",
		Headers:     `{"myheaderkey": "myheadervalue"}`,
		Attempts:    0,
		MaxAttempts: 10,
	}

	data := &Message{
		Content: `{"foo":"bar"}`,
		Meta:    *meta,
	}

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal       // Only wait for the leader to ack
	config.Producer.Compression = sarama.CompressionSnappy   // Compress messages
	config.Producer.Flush.Frequency = 500 * time.Millisecond // Flush batches every 500ms

	brokerList := strings.Split("localhost:9092", ",")
	accessLogProducer, err := sarama.NewAsyncProducer(brokerList, config)
	if err != nil {
		fmt.Printf("sarama.NewAsyncProducer failed, err=%s", err)
		t.Error(err)
	}
	accessLogProducer.Input() <- &sarama.ProducerMessage{
		Topic: "rpcallback",
		Key:   sarama.StringEncoder("RECHARGE_SUCCESS"),
		Value: data,
	}
	defer accessLogProducer.Close()

	t.Log("Successfully pushed a message.")
}

func TestPushWithdrawSucc(t *testing.T) {
	meta := &MessageMeta{
		Url:     "http://api.jiesuan.local:8083/api/payment/v1/test-notification",
		Headers: `{"Content-Type":"application/x-www-form-urlencoded", "dealer-id":"push_test_dealer_id", "request-id":"148707994807304192"}`,
	}

	data := &Message{
		Content: `{"data":"vcfFlqYts3hvACDWQWewQ2mrDGVnvbYnriwoVIFMRTqGmU1odWk4QpAYtui4qRU3wNvClVV+sF+gum6egOlMsjg/QqZt+BnNHTbT2VhjxhDnEz2noVqSmdQaugJz7xX8VY39v480mCxYnY9vKgE2nAtzBktxHT7ha2RUSYuzKcFThTEX33BQZwJ7zAZ9nYh08viuxtWAVc0BvM1AcQKmQW3xQQKHLLj4RredgFg2Y+9P41OdKgskSXJNPWi3Oz44wS8Rp5cLMc1JLkQ14qvoWUG1MSKONTYwTkKs1D6046HTUc2/M1gO3ouf5kqFws5/10zeG7OOSNDMtvtMLwKzNwkB1+B0dm6iCoCypcjssmlPQW4Uoa67d8C3zJtijDBvj8Np38Fq0awXw4EVwEs0wNil9/ROk8+vCvscFAeNedtd52+jodaoH/uF5/qTO7Ec02+NAMYSDkk+Wj4g12M4QS4aco9tg8SZ7EUwcQpuzcU2rhN5M58sOc1yIjS8VcLTuI+dGrytrckoNBoug2ZyHQye5qlAjIja2wsTSCvzRDCS1K/Iab0eJ6dE8/gg98wnur0zssS7mBahx/z8SZXb8w==","mess":"939984059","timestamp":"1496312014","sign":"bc33654d1711c49846ca466dfbdb6541b0172ecbb6ff242a6a77e94b121f127b","sign_type":"sha256"}`,
		Meta:    *meta,
	}

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal       // Only wait for the leader to ack
	config.Producer.Compression = sarama.CompressionSnappy   // Compress messages
	config.Producer.Flush.Frequency = 500 * time.Millisecond // Flush batches every 500ms

	brokerList := strings.Split("localhost:9092", ",")
	accessLogProducer, err := sarama.NewAsyncProducer(brokerList, config)
	if err != nil {
		fmt.Printf("sarama.NewAsyncProducer failed, err=%s", err)
		t.Error(err)
	}

	accessLogProducer.Input() <- &sarama.ProducerMessage{
		Topic: "geass-callback",
		Value: data,
	}
	defer accessLogProducer.Close()

	t.Log("Successfully pushed a message.")
}

func TestCheckUrl(t *testing.T) {
	assert := assert.New(t)

	var err error
	var testUrl string

	testUrl = "hehe"
	err = checkUrl(testUrl)
	assert.NotNil(err)
	t.Logf("url=%s, err=%s", testUrl, err)

	testUrl = "错误的地址"
	err = checkUrl(testUrl)
	assert.NotNil(err)
	t.Logf("url=%s, err=%s", testUrl, err)

	testUrl = "http//hehe.com"
	err = checkUrl(testUrl)
	assert.NotNil(err)
	t.Logf("url=%s, err=%s", testUrl, err)

	testUrl = "https//hehe.com"
	err = checkUrl(testUrl)
	assert.NotNil(err)
	t.Logf("url=%s, err=%s", testUrl, err)

	testUrl = "ht://hehe.com"
	err = checkUrl(testUrl)
	assert.Nil(err)
	t.Logf("url=%s, err=%s", testUrl, err)

	testUrl = "http:hehe.com"
	err = checkUrl(testUrl)
	assert.Nil(err)
	t.Logf("url=%s, err=%s", testUrl, err)

	testUrl = "http://hehe.com"
	err = checkUrl(testUrl)
	assert.Nil(err)
	t.Logf("url=%s, err=%s", testUrl, err)
}

func TestDebugMissing(t *testing.T) {
	data := &Message{
		Content: `{"错":"误"}`,
		Meta: MessageMeta{
			Url:     "错误链接",
			Headers: `{"myheaderkey": "wrong"}`,
		},
	}
	data1 := &Message{
		Content: `{"正":"确"}`,
		Meta: MessageMeta{
			Url:     "http://localhost:8000",
			Headers: `{"myheaderkey": "correct"}`,
		},
	}

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal       // Only wait for the leader to ack
	config.Producer.Compression = sarama.CompressionSnappy   // Compress messages
	config.Producer.Flush.Frequency = 500 * time.Millisecond // Flush batches every 500ms

	brokerList := strings.Split("localhost:9092", ",")
	accessLogProducer, err := sarama.NewAsyncProducer(brokerList, config)
	if err != nil {
		fmt.Printf("sarama.NewAsyncProducer failed, err=%s", err)
		t.Error(err)
	}

	for i := 1; i <= 1024; i++ {
		if i%2 == 0 {
			accessLogProducer.Input() <- &sarama.ProducerMessage{
				Topic: "geass-callback",
				Value: data1,
			}
		} else {
			accessLogProducer.Input() <- &sarama.ProducerMessage{
				Topic: "geass-callback",
				Value: data,
			}
		}
		time.Sleep(time.Millisecond)
	}
	defer accessLogProducer.Close()

	t.Log("Successfully pushed a message.")
}
