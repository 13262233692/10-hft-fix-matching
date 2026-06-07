package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"hft-fix-matching/fix"
	"hft-fix-matching/gateway"
	"hft-fix-matching/matching"
	"hft-fix-matching/storage"
)

type Engine struct {
	books   map[string]*matching.OrderBook
	gateway *gateway.Gateway
	redis   *storage.RedisTradeWriter
}

func NewEngine(redisAddr, redisPassword string, redisDB int, listenAddr string) *Engine {
	e := &Engine{
		books: make(map[string]*matching.OrderBook),
	}

	e.redis = storage.NewRedisTradeWriter(redisAddr, redisPassword, redisDB)

	e.gateway = gateway.NewGateway(listenAddr, "SERVER", "CLIENT", e.handleMessage)

	return e
}

func (e *Engine) getOrCreateBook(symbol string) *matching.OrderBook {
	if book, ok := e.books[symbol]; ok {
		return book
	}
	book := matching.NewOrderBook(symbol)
	book.SetTradeListener(func(trade *matching.Trade) {
		if err := e.redis.WriteTrade(trade); err != nil {
			log.Printf("[Engine] Redis write error: %v", err)
		}
		log.Printf("[Engine] Trade: %s %s Price=%.4f Qty=%d Buyer=%s Seller=%s",
			trade.TradeID, trade.Symbol, trade.Price, trade.Quantity,
			trade.BuyOrderID, trade.SellOrderID)
	})
	e.books[symbol] = book
	return book
}

func (e *Engine) handleMessage(msg *fix.Message, conn net.Conn) {
	connID := conn.RemoteAddr().String()
	msgType := msg.MsgType()

	switch msgType {
	case fix.MsgTypeLogon:
		e.handleLogon(msg, conn, connID)

	case fix.MsgTypeNewOrderSingle:
		e.handleNewOrder(msg, conn, connID)

	case fix.MsgTypeOrderCancelRequest:
		e.handleCancel(msg, conn, connID)

	case fix.MsgTypeHeartbeat:
		log.Printf("[Engine] Heartbeat from %s", connID)

	case fix.MsgTypeTestRequest:
		e.gateway.SendHeartbeat(conn, connID)

	case fix.MsgTypeLogout:
		log.Printf("[Engine] Logout from %s", connID)
		conn.Close()

	default:
		log.Printf("[Engine] Unknown msg type: %s from %s", msgType, connID)
		seqNum, _ := msg.GetInt(fix.TagMsgSeqNum)
		e.gateway.SendReject(conn, connID, seqNum, fmt.Sprintf("Unsupported MsgType: %s", msgType))
	}
}

func (e *Engine) handleLogon(msg *fix.Message, conn net.Conn, connID string) {
	logon, err := fix.DecodeLogon(msg)
	if err != nil {
		log.Printf("[Engine] Logon decode error from %s: %v", connID, err)
		return
	}
	log.Printf("[Engine] Logon from %s: Sender=%s Target=%s HeartBtInt=%d",
		connID, logon.SenderCompID, logon.TargetCompID, logon.HeartBtInt)

	e.gateway.SendLogonResponse(conn, connID)
}

func (e *Engine) handleNewOrder(msg *fix.Message, conn net.Conn, connID string) {
	order, err := fix.DecodeNewOrderSingle(msg)
	if err != nil {
		log.Printf("[Engine] NewOrderSingle decode error from %s: %v", connID, err)
		seqNum, _ := msg.GetInt(fix.TagMsgSeqNum)
		e.gateway.SendReject(conn, connID, seqNum, err.Error())
		return
	}

	book := e.getOrCreateBook(order.Symbol)

	internalOrder := &matching.Order{
		ID:        order.ClOrdID,
		Symbol:    order.Symbol,
		Side:      int(order.Side),
		Price:     order.Price,
		Quantity:  order.OrderQty,
		Remaining: order.OrderQty,
		OrdType:   int(order.OrdType),
		Timestamp: time.Now().UnixNano(),
		ConnID:    connID,
	}

	start := time.Now()
	trades := book.AddOrder(internalOrder)
	elapsed := time.Since(start)

	if len(trades) > 0 {
		book.ProcessTrades(trades)

		var cumQty int64
		var avgPrice float64
		var totalValue float64
		for _, t := range trades {
			cumQty += t.Quantity
			totalValue += t.Price * float64(t.Quantity)
		}
		if cumQty > 0 {
			avgPrice = totalValue / float64(cumQty)
		}

		e.gateway.SendExecutionReport(conn, connID, order.ClOrdID, order.Symbol,
			int(order.Side), int(order.OrdType), order.Price, order.OrderQty,
			cumQty, avgPrice, "F", "1")

		if internalOrder.Remaining == 0 {
			e.gateway.SendExecutionReport(conn, connID, order.ClOrdID, order.Symbol,
				int(order.Side), int(order.OrdType), order.Price, order.OrderQty,
				cumQty, avgPrice, "F", "2")
		}
	}

	if internalOrder.Remaining > 0 && internalOrder.OrdType == 2 {
		e.gateway.SendExecutionReport(conn, connID, order.ClOrdID, order.Symbol,
			int(order.Side), int(order.OrdType), order.Price, order.OrderQty,
			order.OrderQty-internalOrder.Remaining, order.Price, "0", "0")
	}

	log.Printf("[Engine] Order %s %s Side=%d Type=%d Price=%.4f Qty=%d Matched=%d Latency=%s",
		order.ClOrdID, order.Symbol, order.Side, order.OrdType, order.Price,
		order.OrderQty, len(trades), elapsed)

	if elapsed > time.Millisecond {
		log.Printf("[Engine] WARNING: Matching latency exceeded 1ms: %s", elapsed)
	}
}

func (e *Engine) handleCancel(msg *fix.Message, conn net.Conn, connID string) {
	cancel, err := fix.DecodeOrderCancelRequest(msg)
	if err != nil {
		log.Printf("[Engine] CancelRequest decode error from %s: %v", connID, err)
		return
	}

	book, ok := e.books[cancel.Symbol]
	if !ok {
		log.Printf("[Engine] Cancel rejected: no book for %s", cancel.Symbol)
		return
	}

	removed := book.CancelOrder(cancel.OrigClOrdID)
	if removed {
		log.Printf("[Engine] Cancel success: %s from %s", cancel.OrigClOrdID, connID)
	} else {
		log.Printf("[Engine] Cancel failed: order %s not found", cancel.OrigClOrdID)
	}
}

func (e *Engine) Start() error {
	log.Println("[Engine] Starting HFT FIX Matching Engine...")
	if err := e.gateway.Start(); err != nil {
		return fmt.Errorf("gateway start: %w", err)
	}
	log.Println("[Engine] Engine started successfully")
	return nil
}

func (e *Engine) Stop() {
	log.Println("[Engine] Shutting down...")
	e.gateway.Stop()
	if e.redis != nil {
		e.redis.Close()
	}
}

func main() {
	redisAddr := getEnv("REDIS_ADDR", "localhost:6379")
	redisPassword := getEnv("REDIS_PASSWORD", "")
	listenAddr := getEnv("FIX_LISTEN", ":9876")

	redisDB := 0

	engine := NewEngine(redisAddr, redisPassword, redisDB, listenAddr)

	if err := engine.Start(); err != nil {
		log.Fatalf("[Engine] Failed to start: %v", err)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	engine.Stop()
	log.Println("[Engine] Shutdown complete")
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
