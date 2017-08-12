package notification

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	neturl "net/url"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/go-redis/redis"
	"github.com/golang/glog"
)

const (
	E_CAPPED = "The attempts has been capped"
)

// 执行消息发送 (http post), 注意该程序只对消息进行发送, 不改变消息本身
// msg 表示 kafka 原始消息
// dest 如果发送失败, 那么将 offset 写入(传递)到该 redis list (RPUSH)
// retryData 重试所需的数据, 并且用于写入到 redis hash (HMSET)
func Fire(_redis *redis.Client, msg *sarama.ConsumerMessage, dest string, retryData MessageRetry) (err error) {
	fn := "Fire"
	glog.Infof("@%s, kafka message=%+v", fn, msg)

	// 10 json 解码
	var message Message
	err = json.Unmarshal(msg.Value, &message)
	if err != nil {
		glog.Errorf("@%s, message does not json format, msg.Value:%v\n", msg.Value)
		return
	}
	glog.Infof("@%s, human readable message=%+v", fn, message)

	// 20 检查 URL 正确性
	if err = checkUrl(message.Meta.Url); err != nil {
		glog.Infof("@%s, 通知地址不正确, 不通知, message=%+v, err=%s", fn, message, err)
		return
	}

	// 30 HTTP 请求并预防一般性网络出错
	var result string
	sleepTime := time.Second * 1
	for i := 1; i <= 3; i++ {
		if result, err = post(message.Content, message.Meta.Url, message.Meta.Headers); err == nil {
			break
		}
		glog.Infof("@%s, retrying, current attempts is: %d sleepTime: %v", fn, i, sleepTime)
		time.Sleep(sleepTime)
	}

	// 40 检查请求返回是否如期望
	needRetry := checkResult(result)

	if needRetry == false {
		glog.Infof("@%s, post success, message=%v, response=%s", fn, message, result)
		return
	} else {
		glog.Infof("@%s, post failed, message=%v, response=%s", fn, message, result)
	}

	// 50 放入重试列表
	if retryData == (MessageRetry{}) {
		retryData.Offset = msg.Offset
		retryData.Partition = msg.Partition
		retryData.Attempts = int32(0)
		retryData.NextTime = int64(0)
		dest = fmt.Sprintf(FORMAT_LIST, msg.Topic, 2, "4m")
	}
	if err = gotoRetry(_redis, msg.Topic, retryData, dest); err != nil {
		if fmt.Sprint(err) == E_CAPPED {
			glog.Warningf("@%s, The attempts has been capped, message=%v, response=%s", fn, message, result)
			err = nil
		} else {
			glog.Errorf("@%s, gotoRetry failed, err=%s, topic=%s, retryData=%+v, dest=%s", fn, err, msg.Topic, retryData, dest)
		}
		return
	}

	return
}

func gotoRetry(_redis *redis.Client, topic string, retryData MessageRetry, dest string) (err error) {
	fn := "gotoRetry"

	// 10 如果 dest 为空表示最后一次通知完成则不再继续通知
	if dest == "" {
		err = errors.New(E_CAPPED)
		return
	}

	// 20 增加尝试次数和计算下一次通知时间
	var intervalStr string
	retryData.Attempts += 1
	retryData.NextTime, intervalStr = getNextTime(retryData.Attempts)

	// 30 追加到 redis list (RPUSH)
	listKey := fmt.Sprintf(FORMAT_LIST, topic, retryData.Attempts+1, intervalStr)
	hashKey := fmt.Sprintf(FORMAT_HASH, topic, retryData.Offset)

	if _, err = _redis.RPush(listKey, retryData.Offset).Result(); err != nil {
		glog.Errorf("@%s, _redis.RPush failed, err=%s, key=%s, offset=%d", fn, err, listKey, retryData.Offset)
		return
	}
	if _, err = _redis.HMSet(hashKey, retryData.Fields()).Result(); err != nil {
		glog.Errorf("@%s, _redis.HMSet failed, err=%s, key=%s, fields=%+v", fn, err, hashKey, retryData.Fields())
		return
	}
	if _, err = _redis.ExpireAt(hashKey, time.Now().AddDate(0, 0, 7)).Result(); err != nil {
		glog.Errorf("@%s, _redis.ExpireAt failed, err=%s, key=%s, time=%s", fn, err, hashKey, time.Now().AddDate(0, 0, 7))
		return
	}
	return
}

// 下一次尝试时间
// @link https://github.com/YunzhanghuOpen/notification/issues/4
func getNextTime(attempted int32) (nextTime int64, intervalStr string) {
	now := time.Now()
	switch attempted {
	case 1:
		nextTime = now.Add(time.Minute * 4).Unix()
		intervalStr = "4m"
	case 2:
		nextTime = now.Add(time.Minute * 10).Unix()
		intervalStr = "10m"
	case 3:
		nextTime = now.Add(time.Minute * 10).Unix()
		intervalStr = "10m"
	case 4:
		nextTime = now.Add(time.Hour * 1).Unix()
		intervalStr = "1h"
	case 5:
		nextTime = now.Add(time.Hour * 2).Unix()
		intervalStr = "2h"
	case 6:
		nextTime = now.Add(time.Hour * 6).Unix()
		intervalStr = "6h"
	case 7:
		nextTime = now.Add(time.Hour * 15).Unix()
		intervalStr = "15h"
	default:
		nextTime = now.Add(time.Minute * 4).Unix()
		intervalStr = "4m"
	}
	return
}

func checkUrl(url string) (err error) {
	_, err = neturl.ParseRequestURI(url)
	return
}

func post(jsonData string, url string, header string) (result string, err error) {
	fn := "post"
	glog.Infof("@%s, url=%s, jsonData=%s, header=%v", fn, url, jsonData, header)

	var headersMap map[string]string
	if header != "" {
		if err := json.Unmarshal([]byte(header), &headersMap); err != nil {
			glog.Errorf("@%s, json.Unmarshal header failed, header=%s", fn, header)
			return "", err
		}
	}

	var (
		req     *http.Request
		payload *strings.Reader
	)

	// 如果是自定义的 Content-Type: application/x-www-form-urlencoded 类型
	type1, ok1 := headersMap["Content-Type"]
	type2, ok2 := headersMap["content-type"]
	if (ok1 || ok2) && (type1 == "application/x-www-form-urlencoded" || type2 == "application/x-www-form-urlencoded") {
		glog.Infof("@%s, custom content-type:%s | %s", fn, type1, type2)

		var dataMap map[string]string
		if err := json.Unmarshal([]byte(jsonData), &dataMap); err != nil {
			glog.Errorf("@%s, json.Unmarshal failed, jsonData:%s", fn, jsonData)
			return "", err
		}
		glog.Infof("@%s, unmarshal dataMap=%+v", fn, dataMap)

		v := neturl.Values{}
		for key, val := range dataMap {
			v.Add(key, val)
		}
		payload = strings.NewReader(v.Encode())
		req, _ = http.NewRequest("POST", url, payload)
	} else {
		// 默认 Content-Type: application/json
		payload = strings.NewReader(jsonData)
		req, _ = http.NewRequest("POST", url, payload)
		req.Header.Add("content-type", "application/json")
	}

	for k, v := range headersMap {
		req.Header.Add(k, v)
	}

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		glog.Errorf("@%s, http.DefaultClient.Do(req), err=%s, req=%+v", fn, err, req)
		return "", err
	}

	defer res.Body.Close()
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		glog.Errorf("@%s, ioutil.ReadAll(res.Body), err=%s, res.Body=%+v", fn, err, res.Body)
		return "", err
	}

	result = string(body)
	glog.Infof("@%s, post: res = %v body = %v", fn, res, result)

	return result, nil
}

func checkResult(ret string) (needRetry bool) {
	fn := "checkResult"

	// 可接受的返回，类型1，success
	var result1 string = "success"
	if ret == result1 {
		glog.Infof("@%s, check result, success", fn)
		return
	}

	// 可接受的返回，类型2，code:0000
	type Result2 struct {
		Code      string `json:"code"`       // 返回状态码，0000表示获取成功，非0000表示失败
		Message   string `json:"message"`    // 错误信息
		RequestId string `json:"request_id"` // request_id
	}
	var result2 Result2
	if err := json.Unmarshal([]byte(ret), &result2); err != nil {
		glog.Warningf("@%s, json.Unmarshal failed, err=%s, data=%s", fn, err, ret)
	} else {
		if result2.Code == "0000" {
			glog.Infof("@%s, check result, success", fn)
			return
		}
	}

	// 以上都不属于
	needRetry = true

	return
}
