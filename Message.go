package notification

import "encoding/json"

type Message struct {
	Content string      `json:"content"`
	Meta    MessageMeta `json:"meta"`

	encoded []byte `json:"-"`
	err     error  `json:"-"`
}

func (ale *Message) ensureEncoded() {
	if ale.encoded == nil && ale.err == nil {
		ale.encoded, ale.err = json.Marshal(ale)
	}
}

func (ale *Message) Length() int {
	ale.ensureEncoded()
	return len(ale.encoded)
}

func (ale *Message) Encode() ([]byte, error) {
	ale.ensureEncoded()
	return ale.encoded, ale.err
}
