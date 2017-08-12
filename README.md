notification
============

基于 kafka 和 redis 制作的消息通知程序

## 编译和安装

```shell
$ make
go get github.com/Shopify/sarama
go get github.com/golang/glog
go get github.com/go-redis/redis
go get github.com/go-yaml/yaml
go get github.com/stretchr/testify/assert
gofmt -l -w -s ./
go build -ldflags "-w -s" -o bin/listener ./main/listener.go
go build -ldflags "-w -s" -o bin/listener-retry ./retry/retry.go
```

##  启动服务

### 1. 手动运行服务

- 实时处理 `./bin/listener -brokers kafka:9092 -topic mytopic -verbose -offset newest --stderrthreshold INFO`
- 重试处理 `./bin/listener-retry -brokers kafka:9092 -topic mytopic -verbose --stderrthreshold INFO -v 20`

### 2. 使用 Supervisor

- Supervisor 版本：v3.3.1

- 启动：`supervisorctl start notification`

- 配置：
  ```shell
  # 消息通知
  cat /etc/supervisor/conf.d/notification.ini
  [program:notification]
  command=/home/www/notification/bin/listener -brokers 10.253.40.221:9092,10.253.41.10:9092,10.253.40.232:9092 -topic mytopic -verbose -offset newest -log_dir /home/www/notification/log
  autorestart=true
  # 消息通知重试
  cat /etc/supervisor/conf.d/notification-retry.ini
  [program:notification-retry]
  command=/home/www/notification/bin/listener-retry -brokers 10.253.40.221:9092,10.253.41.10:9092,10.253.40.232:9092 -topic mytopic -verbose -log_dir /home/www/notification/log
  autorestart=true
  ```

## 日志搜索

- 完整的 kafka 消息: `glog.Infof("@%s, human readable message=%+v", fn, message)`
- json decoded 后的消息内容: `glog.Infof("@%s, post success, message=%v, response=%s", fn, message, result)`
