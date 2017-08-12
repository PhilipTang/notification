package notification

const (
	FORMAT_LIST = "%s-list-attempts-%d-%s"
	FORMAT_HASH = "%s-hash-offset-%d"
)

type MessageRetry struct {
	Offset    int64 // 消息所在 offset
	Partition int32 // 消息所在 partition
	Attempts  int32 // 已尝试次数
	NextTime  int64 // 下一次尝试时间(Unix 时间戳)
}

func (p *MessageRetry) Fields() map[string]interface{} {
	return map[string]interface{}{
		"offset":    p.Offset,
		"partition": p.Partition,
		"attempts":  p.Attempts,
		"next_time": p.NextTime,
	}
}
