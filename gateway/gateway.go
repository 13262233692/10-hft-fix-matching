package gateway

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"hft-fix-matching/fix"
)

type MessageHandler func(msg *fix.Message, conn net.Conn)

type Gateway struct {
	addr          string
	server        net.Listener
	handler       MessageHandler
	connections   sync.Map
	connCount     atomic.Int64
	seqNums       sync.Map
	senderCompID  string
	targetCompID  string
	ctx           context.Context
	cancel        context.CancelFunc
}

func NewGateway(addr, senderCompID, targetCompID string, handler MessageHandler) *Gateway {
	ctx, cancel := context.WithCancel(context.Background())
	return &Gateway{
		addr:         addr,
		handler:      handler,
		senderCompID: senderCompID,
		targetCompID: targetCompID,
		ctx:          ctx,
		cancel:       cancel,
	}
}

func (g *Gateway) Start() error {
	ln, err := net.Listen("tcp", g.addr)
	if err != nil {
		return fmt.Errorf("listen failed: %w", err)
	}
	g.server = ln
	log.Printf("[Gateway] Listening on %s", g.addr)

	go g.acceptLoop()
	return nil
}

func (g *Gateway) Stop() {
	g.cancel()
	if g.server != nil {
		g.server.Close()
	}
	g.connections.Range(func(key, value interface{}) bool {
		conn := value.(net.Conn)
		conn.Close()
		return true
	})
}

func (g *Gateway) acceptLoop() {
	for {
		select {
		case <-g.ctx.Done():
			return
		default:
		}

		conn, err := g.server.Accept()
		if err != nil {
			if g.ctx.Err() != nil {
				return
			}
			log.Printf("[Gateway] Accept error: %v", err)
			continue
		}

		connID := conn.RemoteAddr().String()
		g.connections.Store(connID, conn)
		g.connCount.Add(1)
		log.Printf("[Gateway] New connection from %s, total: %d", connID, g.connCount.Load())

		go g.handleConnection(conn, connID)
	}
}

func (g *Gateway) handleConnection(conn net.Conn, connID string) {
	defer func() {
		conn.Close()
		g.connections.Delete(connID)
		g.connCount.Add(-1)
		log.Printf("[Gateway] Connection %s closed, total: %d", connID, g.connCount.Load())
	}()

	reader := bufio.NewReader(conn)
	var buffer bytes.Buffer

	for {
		select {
		case <-g.ctx.Done():
			return
		default:
		}

		conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		line, err := reader.ReadBytes(byte(fix.SOH))
		if err != nil {
			if err == io.EOF {
				return
			}
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				g.SendHeartbeat(conn, connID)
				continue
			}
			log.Printf("[Gateway] Read error from %s: %v", connID, err)
			return
		}

		buffer.Write(line)

		for {
			msg, remaining, found := g.extractMessage(buffer.Bytes())
			if !found {
				break
			}
			buffer.Reset()
			buffer.Write(remaining)

			if !fix.ValidateChecksum(msg) {
				log.Printf("[Gateway] Checksum validation failed from %s", connID)
				continue
			}

			fixMsg, err := fix.ParseMessage(msg)
			if err != nil {
				log.Printf("[Gateway] Parse error from %s: %v", connID, err)
				continue
			}

			g.handler(fixMsg, conn)
		}
	}
}

func (g *Gateway) extractMessage(data []byte) (message, remaining []byte, found bool) {
	beginIdx := bytes.Index(data, []byte("8=FIX"))
	if beginIdx < 0 {
		return nil, data, false
	}

	bodyLenIdx := bytes.Index(data[beginIdx:], []byte{fix.SOH, '9', '='})
	if bodyLenIdx < 0 {
		return nil, data[beginIdx:], false
	}
	bodyLenIdx += beginIdx + 1

	eqIdx := bytes.Index(data[bodyLenIdx:], []byte{'='})
	if eqIdx < 0 {
		return nil, data[beginIdx:], false
	}
	eqIdx += bodyLenIdx + 1

	sohIdx := bytes.Index(data[eqIdx:], []byte{fix.SOH})
	if sohIdx < 0 {
		return nil, data[beginIdx:], false
	}
	sohIdx += eqIdx

	bodyLenStr := string(data[eqIdx:sohIdx])
	var bodyLen int
	fmt.Sscanf(bodyLenStr, "%d", &bodyLen)

	bodyStart := sohIdx + 1
	trailerLen := 7 + 3
	totalLen := bodyStart + bodyLen + trailerLen

	if len(data[beginIdx:]) < totalLen {
		return nil, data[beginIdx:], false
	}

	msgEnd := beginIdx + totalLen
	endSOH := bytes.Index(data[msgEnd-1:], []byte{fix.SOH})
	if endSOH < 0 {
		return nil, data[beginIdx:], false
	}

	return data[beginIdx : msgEnd], data[msgEnd:], true
}

func (g *Gateway) SendHeartbeat(conn net.Conn, connID string) {
	seqNum := g.getNextSeqNum(connID)
	msg := fix.BuildMessage(fix.MsgTypeHeartbeat, g.senderCompID, g.targetCompID, seqNum, "")
	conn.Write(msg)
}

func (g *Gateway) getNextSeqNum(connID string) int {
	val, _ := g.seqNums.LoadOrStore(connID, new(int32))
	ptr := val.(*int32)
	return int(atomic.AddInt32(ptr, 1))
}

func (g *Gateway) SendLogonResponse(conn net.Conn, connID string) {
	seqNum := g.getNextSeqNum(connID)
	body := fmt.Sprintf("98=0%c108=30%c", fix.SOH, fix.SOH)
	msg := fix.BuildMessage(fix.MsgTypeLogon, g.senderCompID, g.targetCompID, seqNum, body)
	conn.Write(msg)
	log.Printf("[Gateway] Sent Logon response to %s", connID)
}

func (g *Gateway) SendReject(conn net.Conn, connID string, refSeqNum int, text string) {
	seqNum := g.getNextSeqNum(connID)
	body := fmt.Sprintf("45=%d%c58=%s%c", refSeqNum, fix.SOH, text, fix.SOH)
	msg := fix.BuildMessage("3", g.senderCompID, g.targetCompID, seqNum, body)
	conn.Write(msg)
}

func (g *Gateway) SendExecutionReport(conn net.Conn, connID string, clOrdID, symbol string, side int, ordType int, price float64, orderQty int64, cumQty int64, avgPrice float64, execType string, ordStatus string) {
	seqNum := g.getNextSeqNum(connID)
	body := fmt.Sprintf("37=%s%c11=%s%c55=%s%c54=%d%c40=%d%c44=%g%c38=%d%c14=%d%c6=%g%c150=%s%c39=%s%c",
		clOrdID, fix.SOH, clOrdID, fix.SOH, symbol, fix.SOH, side, fix.SOH,
		ordType, fix.SOH, price, fix.SOH, orderQty, fix.SOH, cumQty, fix.SOH,
		avgPrice, fix.SOH, execType, fix.SOH, ordStatus, fix.SOH)
	msg := fix.BuildMessage("8", g.senderCompID, g.targetCompID, seqNum, body)
	conn.Write(msg)
}
