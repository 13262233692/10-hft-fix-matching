package fix

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
)

const SOH = 0x01

const (
	TagBeginString   = 8
	TagBodyLength    = 9
	TagMsgType       = 35
	TagSenderCompID  = 49
	TagTargetCompID  = 56
	TagMsgSeqNum     = 34
	TagChecksum      = 10

	TagClOrdID       = 11
	TagSymbol        = 55
	TagSide          = 54
	TagOrdType       = 40
	TagPrice         = 44
	TagOrderQty      = 38
	TagOrigClOrdID   = 41
	TagEncryptMethod = 98
	TagHeartBtInt    = 108
	TagPassword      = 554
	TagMaxFloor      = 111

	MsgTypeLogon        = "A"
	MsgTypeNewOrderSingle = "D"
	MsgTypeOrderCancelRequest = "F"
	MsgTypeHeartbeat    = "0"
	MsgTypeTestRequest  = "1"
	MsgTypeLogout       = "5"
)

type Side int

const (
	SideBuy  Side = 1
	SideSell Side = 2
)

type OrdType int

const (
	OrdTypeMarket  OrdType = 1
	OrdTypeLimit   OrdType = 2
	OrdTypeIceberg OrdType = 3
)

type Message struct {
	Fields map[int]string
	Raw    []byte
}

type Logon struct {
	EncryptMethod int
	HeartBtInt    int
	Password      string
	SenderCompID  string
	TargetCompID  string
}

type NewOrderSingle struct {
	ClOrdID  string
	Symbol   string
	Side     Side
	OrdType  OrdType
	Price    float64
	OrderQty int64
	MaxFloor int64
}

type OrderCancelRequest struct {
	OrigClOrdID string
	ClOrdID     string
	Symbol      string
	Side        Side
}

func ParseMessage(data []byte) (*Message, error) {
	msg := &Message{
		Fields: make(map[int]string),
		Raw:    make([]byte, len(data)),
	}
	copy(msg.Raw, data)

	segments := bytes.Split(data, []byte{SOH})
	for _, seg := range segments {
		if len(seg) == 0 {
			continue
		}
		parts := bytes.SplitN(seg, []byte{'='}, 2)
		if len(parts) != 2 {
			continue
		}
		tag, err := strconv.Atoi(string(parts[0]))
		if err != nil {
			continue
		}
		msg.Fields[tag] = string(parts[1])
	}
	return msg, nil
}

func (m *Message) MsgType() string {
	return m.Fields[TagMsgType]
}

func (m *Message) GetString(tag int) string {
	return m.Fields[tag]
}

func (m *Message) GetInt(tag int) (int, error) {
	s, ok := m.Fields[tag]
	if !ok {
		return 0, fmt.Errorf("tag %d not found", tag)
	}
	return strconv.Atoi(s)
}

func (m *Message) GetFloat64(tag int) (float64, error) {
	s, ok := m.Fields[tag]
	if !ok {
		return 0, fmt.Errorf("tag %d not found", tag)
	}
	return strconv.ParseFloat(s, 64)
}

func (m *Message) GetInt64(tag int) (int64, error) {
	s, ok := m.Fields[tag]
	if !ok {
		return 0, fmt.Errorf("tag %d not found", tag)
	}
	return strconv.ParseInt(s, 10, 64)
}

func ValidateChecksum(data []byte) bool {
	lastSOH := bytes.LastIndex(data, []byte{SOH})
	if lastSOH < 0 {
		return false
	}

	var sum int
	for i := 0; i < lastSOH; i++ {
		sum += int(data[i])
	}
	sum = sum % 256

	checksumStr := ""
	inChecksum := false
	for i := lastSOH + 1; i < len(data); i++ {
		if data[i] == SOH {
			break
		}
		if inChecksum {
			checksumStr += string(data[i])
		}
		if data[i] == '=' {
			inChecksum = true
		}
	}

	if checksumStr == "" {
		return false
	}

	expected, err := strconv.Atoi(strings.TrimSpace(checksumStr))
	if err != nil {
		return false
	}

	return sum == expected
}

func ExtractBodyLength(data []byte) (int, error) {
	segments := bytes.Split(data, []byte{SOH})
	for _, seg := range segments {
		parts := bytes.SplitN(seg, []byte{'='}, 2)
		if len(parts) == 2 && string(parts[0]) == "9" {
			return strconv.Atoi(string(parts[1]))
		}
	}
	return 0, fmt.Errorf("BodyLength not found")
}

func DecodeLogon(msg *Message) (*Logon, error) {
	l := &Logon{}
	em, err := msg.GetInt(TagEncryptMethod)
	if err != nil {
		return nil, fmt.Errorf("EncryptMethod required: %w", err)
	}
	l.EncryptMethod = em

	hbi, err := msg.GetInt(TagHeartBtInt)
	if err != nil {
		return nil, fmt.Errorf("HeartBtInt required: %w", err)
	}
	l.HeartBtInt = hbi

	l.Password = msg.GetString(TagPassword)
	l.SenderCompID = msg.GetString(TagSenderCompID)
	l.TargetCompID = msg.GetString(TagTargetCompID)
	return l, nil
}

func DecodeNewOrderSingle(msg *Message) (*NewOrderSingle, error) {
	o := &NewOrderSingle{}
	o.ClOrdID = msg.GetString(TagClOrdID)
	if o.ClOrdID == "" {
		return nil, fmt.Errorf("ClOrdID required")
	}
	o.Symbol = msg.GetString(TagSymbol)
	if o.Symbol == "" {
		return nil, fmt.Errorf("Symbol required")
	}

	sideVal, err := msg.GetInt(TagSide)
	if err != nil {
		return nil, fmt.Errorf("Side required: %w", err)
	}
	o.Side = Side(sideVal)

	ordTypeVal, err := msg.GetInt(TagOrdType)
	if err != nil {
		return nil, fmt.Errorf("OrdType required: %w", err)
	}
	o.OrdType = OrdType(ordTypeVal)

	if o.OrdType == OrdTypeLimit {
		price, err := msg.GetFloat64(TagPrice)
		if err != nil {
			return nil, fmt.Errorf("Price required for limit order: %w", err)
		}
		o.Price = price
	}

	qty, err := msg.GetInt64(TagOrderQty)
	if err != nil {
		return nil, fmt.Errorf("OrderQty required: %w", err)
	}
	o.OrderQty = qty

	maxFloor, err := msg.GetInt64(TagMaxFloor)
	if err == nil && maxFloor > 0 {
		o.MaxFloor = maxFloor
		o.OrdType = OrdTypeIceberg
		if o.Price == 0 {
			return nil, fmt.Errorf("Price required for iceberg order")
		}
	}

	return o, nil
}

func DecodeOrderCancelRequest(msg *Message) (*OrderCancelRequest, error) {
	r := &OrderCancelRequest{}
	r.OrigClOrdID = msg.GetString(TagOrigClOrdID)
	if r.OrigClOrdID == "" {
		return nil, fmt.Errorf("OrigClOrdID required")
	}
	r.ClOrdID = msg.GetString(TagClOrdID)
	r.Symbol = msg.GetString(TagSymbol)

	sideVal, err := msg.GetInt(TagSide)
	if err == nil {
		r.Side = Side(sideVal)
	}
	return r, nil
}

func BuildMessage(msgType string, senderCompID, targetCompID string, seqNum int, body string) []byte {
	header := fmt.Sprintf("8=FIX.4.4%c9=%d%c35=%s%c49=%s%c56=%s%c34=%d%c",
		SOH, len(body), SOH, msgType, SOH, senderCompID, SOH, targetCompID, SOH, seqNum, SOH)

	var sum int
	full := []byte(header + body)
	for _, b := range full {
		sum += int(b)
	}
	sum = sum % 256

	trailer := fmt.Sprintf("10=%03d%c", sum, SOH)
	return append(full, []byte(trailer)...)
}
