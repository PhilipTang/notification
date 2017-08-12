package notification

import "encoding/json"

type MessageMeta struct {
	Url         string `json:"url"`
	Headers     string `json:"headers"`
	Attempts    int    `json:"attempts"`
	MaxAttempts int    `json:"max_attempts"`

	encoded []byte `json:"-"`
	err     error  `json:"-"`
}

func (ale *MessageMeta) ensureEncoded() {
	if ale.encoded == nil && ale.err == nil {
		ale.encoded, ale.err = json.Marshal(ale)
	}
}

func (ale *MessageMeta) Length() int {
	ale.ensureEncoded()
	return len(ale.encoded)
}

func (ale *MessageMeta) Encode() ([]byte, error) {
	ale.ensureEncoded()
	return ale.encoded, ale.err
}
