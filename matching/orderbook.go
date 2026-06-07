package matching

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

type Trade struct {
	TradeID    string
	Symbol     string
	Price      float64
	Quantity   int64
	BuyOrderID string
	SellOrderID string
	Timestamp  int64
}

type TradeListener func(trade *Trade)

type OrderBook struct {
	Symbol      string
	Bids        *RBTree
	Asks        *RBTree
	orderMap    map[string]*orderLocation
	mu          sync.RWMutex
	tradeListener TradeListener
	orderSeq    atomic.Int64
}

type orderLocation struct {
	Price float64
	Side  int
}

func NewOrderBook(symbol string) *OrderBook {
	return &OrderBook{
		Symbol:   symbol,
		Bids:     NewRBTree(false),
		Asks:     NewRBTree(true),
		orderMap: make(map[string]*orderLocation),
	}
}

func (ob *OrderBook) SetTradeListener(listener TradeListener) {
	ob.tradeListener = listener
}

func (ob *OrderBook) AddOrder(order *Order) []*Trade {
	ob.mu.Lock()
	defer ob.mu.Unlock()

	if order.ID == "" {
		order.ID = fmt.Sprintf("ORD-%d", ob.orderSeq.Add(1))
	}
	if order.Timestamp == 0 {
		order.Timestamp = time.Now().UnixNano()
	}
	order.Remaining = order.Quantity

	var trades []*Trade

	if order.Side == 1 {
		trades = ob.matchBuyOrder(order)
	} else {
		trades = ob.matchSellOrder(order)
	}

	if order.Remaining > 0 && order.OrdType == 2 {
		ob.insertRemaining(order)
	}

	return trades
}

func (ob *OrderBook) matchBuyOrder(order *Order) []*Trade {
	var trades []*Trade

	for order.Remaining > 0 && !ob.Asks.IsEmpty() {
		bestAsks := ob.Asks.BestOrders()
		if bestAsks == nil || bestAsks.Size == 0 {
			break
		}

		bestAskPrice, _ := ob.Asks.BestPrice()
		if order.OrdType == 2 && order.Price < bestAskPrice {
			break
		}

		for bestAsks.Size > 0 && order.Remaining > 0 {
			counterOrder := bestAsks.PeekFirst()
			if counterOrder == nil {
				break
			}

			tradeQty := order.Remaining
			if counterOrder.Remaining < tradeQty {
				tradeQty = counterOrder.Remaining
			}

			trade := &Trade{
				TradeID:     fmt.Sprintf("TRD-%d", ob.orderSeq.Add(1)),
				Symbol:      ob.Symbol,
				Price:       counterOrder.Price,
				Quantity:    tradeQty,
				BuyOrderID:  order.ID,
				SellOrderID: counterOrder.ID,
				Timestamp:   time.Now().UnixNano(),
			}
			trades = append(trades, trade)

			order.Remaining -= tradeQty
			counterOrder.Remaining -= tradeQty

			if counterOrder.Remaining == 0 {
				ob.Asks.PopBestOrder()
				delete(ob.orderMap, counterOrder.ID)
			}
		}

		if bestAsks.Size == 0 {
			bestPrice, _ := ob.Asks.BestPrice()
			node := ob.Asks.Find(bestPrice)
			if node != nil && node.isEmpty() {
				ob.Asks.deleteNode(node)
			}
		}
	}

	return trades
}

func (ob *OrderBook) matchSellOrder(order *Order) []*Trade {
	var trades []*Trade

	for order.Remaining > 0 && !ob.Bids.IsEmpty() {
		bestBids := ob.Bids.BestOrders()
		if bestBids == nil || bestBids.Size == 0 {
			break
		}

		bestBidPrice, _ := ob.Bids.BestPrice()
		if order.OrdType == 2 && order.Price > bestBidPrice {
			break
		}

		for bestBids.Size > 0 && order.Remaining > 0 {
			counterOrder := bestBids.PeekFirst()
			if counterOrder == nil {
				break
			}

			tradeQty := order.Remaining
			if counterOrder.Remaining < tradeQty {
				tradeQty = counterOrder.Remaining
			}

			trade := &Trade{
				TradeID:     fmt.Sprintf("TRD-%d", ob.orderSeq.Add(1)),
				Symbol:      ob.Symbol,
				Price:       counterOrder.Price,
				Quantity:    tradeQty,
				BuyOrderID:  counterOrder.ID,
				SellOrderID: order.ID,
				Timestamp:   time.Now().UnixNano(),
			}
			trades = append(trades, trade)

			order.Remaining -= tradeQty
			counterOrder.Remaining -= tradeQty

			if counterOrder.Remaining == 0 {
				ob.Bids.PopBestOrder()
				delete(ob.orderMap, counterOrder.ID)
			}
		}

		if bestBids.Size == 0 {
			bestPrice, _ := ob.Bids.BestPrice()
			node := ob.Bids.Find(bestPrice)
			if node != nil && node.isEmpty() {
				ob.Bids.deleteNode(node)
			}
		}
	}

	return trades
}

func (ob *OrderBook) insertRemaining(order *Order) {
	loc := &orderLocation{Price: order.Price, Side: order.Side}
	ob.orderMap[order.ID] = loc

	if order.Side == 1 {
		ob.Bids.Insert(order.Price, order)
	} else {
		ob.Asks.Insert(order.Price, order)
	}
}

func (ob *OrderBook) CancelOrder(orderID string) bool {
	ob.mu.Lock()
	defer ob.mu.Unlock()

	loc, ok := ob.orderMap[orderID]
	if !ok {
		return false
	}

	if loc.Side == 1 {
		ob.Bids.RemoveOrder(loc.Price, orderID)
	} else {
		ob.Asks.RemoveOrder(loc.Price, orderID)
	}
	delete(ob.orderMap, orderID)
	return true
}

func (ob *OrderBook) GetBestBid() (float64, bool) {
	ob.mu.RLock()
	defer ob.mu.RUnlock()
	if ob.Bids.IsEmpty() {
		return 0, false
	}
	return ob.Bids.BestPrice()
}

func (ob *OrderBook) GetBestAsk() (float64, bool) {
	ob.mu.RLock()
	defer ob.mu.RUnlock()
	if ob.Asks.IsEmpty() {
		return 0, false
	}
	return ob.Asks.BestPrice()
}

func (ob *OrderBook) GetSpread() (float64, bool) {
	ob.mu.RLock()
	defer ob.mu.RUnlock()
	bid, ok1 := ob.Bids.BestPrice()
	ask, ok2 := ob.Asks.BestPrice()
	if !ok1 || !ok2 {
		return 0, false
	}
	return ask - bid, true
}

func (ob *OrderBook) ProcessTrades(trades []*Trade) {
	if ob.tradeListener == nil {
		return
	}
	for _, trade := range trades {
		ob.tradeListener(trade)
	}
}

func (ob *OrderBook) PrintBook() {
	ob.mu.RLock()
	defer ob.mu.RUnlock()

	log.Printf("=== Order Book: %s ===", ob.Symbol)
	log.Printf("--- Asks (ascending) ---")
	ob.printTree(ob.Asks.Root, ob.Asks)
	log.Printf("--- Bids (descending) ---")
	ob.printTree(ob.Bids.Root, ob.Bids)
}

func (ob *OrderBook) printTree(node *RBNode, tree *RBTree) {
	if node == nil || node == tree.sentinel {
		return
	}
	ob.printTree(node.Left, tree)
	color := "B"
	if node.Color {
		color = "R"
	}
	log.Printf("  Price=%.4f [%s] Orders=%d", node.Price, color, node.Orders.Size)
	ob.printTree(node.Right, tree)
}
