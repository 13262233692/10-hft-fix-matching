package matching

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

type Trade struct {
	TradeID     string
	Symbol      string
	Price       float64
	Quantity    int64
	BuyOrderID  string
	SellOrderID string
	Timestamp   int64
}

type TradeListener func(trade *Trade)

const (
	cmdAdd    = 1
	cmdCancel = 2
)

type command struct {
	op      int
	order   *Order
	orderID string
	result  chan *cmdResult
}

type cmdResult struct {
	trades []*Trade
	ok     bool
}

type priceSnapshot struct {
	Price float64
	Valid bool
}

type orderLocation struct {
	Price     float64
	Side      int
	HiddenQty int64
	MaxFloor  int64
	IsIceberg bool
}

type OrderBook struct {
	Symbol       string
	bids         *RBTree
	asks         *RBTree
	orderMap     map[string]*orderLocation
	cmdCh        chan *command
	done         chan struct{}
	tradeListener TradeListener
	orderSeq     int64
	bestBidVal   atomic.Value
	bestAskVal   atomic.Value
	running      atomic.Bool
}

func NewOrderBook(symbol string) *OrderBook {
	ob := &OrderBook{
		Symbol:   symbol,
		bids:     NewRBTree(false),
		asks:     NewRBTree(true),
		orderMap: make(map[string]*orderLocation),
		cmdCh:    make(chan *command, 65536),
		done:     make(chan struct{}),
	}

	ob.bestBidVal.Store(priceSnapshot{Valid: false})
	ob.bestAskVal.Store(priceSnapshot{Valid: false})

	ob.running.Store(true)
	go ob.eventLoop()

	return ob
}

func (ob *OrderBook) eventLoop() {
	for ob.running.Load() {
		cmd, ok := <-ob.cmdCh
		if !ok {
			return
		}
		switch cmd.op {
		case cmdAdd:
			trades := ob.processAdd(cmd.order)
			cmd.result <- &cmdResult{trades: trades}
		case cmdCancel:
			ok := ob.processCancel(cmd.orderID)
			cmd.result <- &cmdResult{ok: ok}
		}
	}
}

func (ob *OrderBook) AddOrder(order *Order) []*Trade {
	if !ob.running.Load() {
		return nil
	}
	cmd := &command{
		op:     cmdAdd,
		order:  order,
		result: make(chan *cmdResult, 1),
	}
	ob.cmdCh <- cmd
	result := <-cmd.result
	return result.trades
}

func (ob *OrderBook) CancelOrder(orderID string) bool {
	if !ob.running.Load() {
		return false
	}
	cmd := &command{
		op:      cmdCancel,
		orderID: orderID,
		result:  make(chan *cmdResult, 1),
	}
	ob.cmdCh <- cmd
	result := <-cmd.result
	return result.ok
}

func (ob *OrderBook) Stop() {
	if ob.running.CompareAndSwap(true, false) {
		close(ob.cmdCh)
		close(ob.done)
	}
}

func (ob *OrderBook) SetTradeListener(listener TradeListener) {
	ob.tradeListener = listener
}

func (ob *OrderBook) nextSeq() int64 {
	ob.orderSeq++
	return ob.orderSeq
}

func (ob *OrderBook) processAdd(order *Order) []*Trade {
	if order.ID == "" {
		order.ID = fmt.Sprintf("ORD-%d", ob.nextSeq())
	}
	if order.Timestamp == 0 {
		order.Timestamp = time.Now().UnixNano()
	}

	if order.IsIceberg && order.MaxFloor > 0 && order.Quantity > order.MaxFloor {
		order.HiddenQty = order.Quantity - order.MaxFloor
		order.Remaining = order.MaxFloor
	} else {
		order.Remaining = order.Quantity
		order.HiddenQty = 0
	}

	var trades []*Trade

	if order.Side == SideBuy {
		trades = ob.matchBuyOrder(order)
	} else {
		trades = ob.matchSellOrder(order)
	}

	if order.Remaining > 0 && (order.OrdType == OrdTypeLimit || order.OrdType == OrdTypeIceberg) {
		ob.insertRemaining(order)
	}

	ob.updateBestBidSnapshot()
	ob.updateBestAskSnapshot()

	return trades
}

func (ob *OrderBook) matchBuyOrder(order *Order) []*Trade {
	var trades []*Trade

	for order.Remaining > 0 && !ob.asks.IsEmpty() {
		bestAsks := ob.asks.BestOrders()
		if bestAsks == nil || bestAsks.Size == 0 {
			break
		}

		bestAskPrice, _ := ob.asks.BestPrice()
		if (order.OrdType == OrdTypeLimit || order.OrdType == OrdTypeIceberg) && order.Price < bestAskPrice {
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
				TradeID:     fmt.Sprintf("TRD-%d", ob.nextSeq()),
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
				ob.asks.PopBestOrder()
				if counterOrder.IsIceberg && counterOrder.HiddenQty > 0 {
					ob.replenishIceberg(counterOrder)
				} else {
					delete(ob.orderMap, counterOrder.ID)
				}
			}
		}

		if bestAsks.Size == 0 {
			bestPrice, _ := ob.asks.BestPrice()
			node := ob.asks.Find(bestPrice)
			if node != nil && node.isEmpty() {
				ob.asks.deleteNode(node)
			}
		}
	}

	return trades
}

func (ob *OrderBook) matchSellOrder(order *Order) []*Trade {
	var trades []*Trade

	for order.Remaining > 0 && !ob.bids.IsEmpty() {
		bestBids := ob.bids.BestOrders()
		if bestBids == nil || bestBids.Size == 0 {
			break
		}

		bestBidPrice, _ := ob.bids.BestPrice()
		if (order.OrdType == OrdTypeLimit || order.OrdType == OrdTypeIceberg) && order.Price > bestBidPrice {
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
				TradeID:     fmt.Sprintf("TRD-%d", ob.nextSeq()),
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
				ob.bids.PopBestOrder()
				if counterOrder.IsIceberg && counterOrder.HiddenQty > 0 {
					ob.replenishIceberg(counterOrder)
				} else {
					delete(ob.orderMap, counterOrder.ID)
				}
			}
		}

		if bestBids.Size == 0 {
			bestPrice, _ := ob.bids.BestPrice()
			node := ob.bids.Find(bestPrice)
			if node != nil && node.isEmpty() {
				ob.bids.deleteNode(node)
			}
		}
	}

	return trades
}

func (ob *OrderBook) replenishIceberg(order *Order) {
	sliceQty := order.MaxFloor
	if order.HiddenQty < sliceQty {
		sliceQty = order.HiddenQty
	}
	order.HiddenQty -= sliceQty
	order.Remaining = sliceQty
	order.Timestamp = time.Now().UnixNano()

	ob.bids = ob.rebuildTreeIfNeeded(ob.bids, false)
	ob.asks = ob.rebuildTreeIfNeeded(ob.asks, true)

	if order.Side == SideBuy {
		ob.bids.Insert(order.Price, order)
	} else {
		ob.asks.Insert(order.Price, order)
	}

	loc, exists := ob.orderMap[order.ID]
	if exists {
		loc.HiddenQty = order.HiddenQty
	}
}

func (ob *OrderBook) rebuildTreeIfNeeded(tree *RBTree, ascending bool) *RBTree {
	return tree
}

func (ob *OrderBook) insertRemaining(order *Order) {
	loc := &orderLocation{
		Price:     order.Price,
		Side:      order.Side,
		HiddenQty: order.HiddenQty,
		MaxFloor:  order.MaxFloor,
		IsIceberg: order.IsIceberg,
	}
	ob.orderMap[order.ID] = loc

	if order.Side == SideBuy {
		ob.bids.Insert(order.Price, order)
	} else {
		ob.asks.Insert(order.Price, order)
	}
}

func (ob *OrderBook) processCancel(orderID string) bool {
	loc, ok := ob.orderMap[orderID]
	if !ok {
		return false
	}

	var tree *RBTree
	if loc.Side == SideBuy {
		tree = ob.bids
	} else {
		tree = ob.asks
	}

	removed := tree.RemoveOrder(loc.Price, orderID)
	if !removed {
		delete(ob.orderMap, orderID)
		return false
	}

	if loc.IsIceberg && loc.HiddenQty > 0 {
		loc.HiddenQty = 0
	}

	delete(ob.orderMap, orderID)

	ob.updateBestBidSnapshot()
	ob.updateBestAskSnapshot()

	return true
}

func (ob *OrderBook) updateBestBidSnapshot() {
	if ob.bids.IsEmpty() {
		ob.bestBidVal.Store(priceSnapshot{Valid: false})
	} else {
		price, _ := ob.bids.BestPrice()
		ob.bestBidVal.Store(priceSnapshot{Price: price, Valid: true})
	}
}

func (ob *OrderBook) updateBestAskSnapshot() {
	if ob.asks.IsEmpty() {
		ob.bestAskVal.Store(priceSnapshot{Valid: false})
	} else {
		price, _ := ob.asks.BestPrice()
		ob.bestAskVal.Store(priceSnapshot{Price: price, Valid: true})
	}
}

func (ob *OrderBook) GetBestBid() (float64, bool) {
	snap := ob.bestBidVal.Load().(priceSnapshot)
	return snap.Price, snap.Valid
}

func (ob *OrderBook) GetBestAsk() (float64, bool) {
	snap := ob.bestAskVal.Load().(priceSnapshot)
	return snap.Price, snap.Valid
}

func (ob *OrderBook) GetSpread() (float64, bool) {
	bidSnap := ob.bestBidVal.Load().(priceSnapshot)
	askSnap := ob.bestAskVal.Load().(priceSnapshot)
	if !bidSnap.Valid || !askSnap.Valid {
		return 0, false
	}
	return askSnap.Price - bidSnap.Price, true
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
	log.Printf("=== Order Book: %s ===", ob.Symbol)
	log.Printf("--- Asks (ascending) ---")
	ob.printTree(ob.asks.Root, ob.asks)
	log.Printf("--- Bids (descending) ---")
	ob.printTree(ob.bids.Root, ob.bids)
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
	suffix := ""
	cur := node.Orders.Head
	for cur != nil {
		if cur.Order.IsIceberg {
			suffix = fmt.Sprintf(" [ICEBERG display=%d hidden=%d]", cur.Order.Remaining, cur.Order.HiddenQty)
			break
		}
		cur = cur.Next
	}
	log.Printf("  Price=%.4f [%s] Orders=%d%s", node.Price, color, node.Orders.Size, suffix)
	ob.printTree(node.Right, tree)
}

const NumStripes = 64

type StripedLock struct {
	locks [NumStripes]sync.Mutex
}

func NewStripedLock() *StripedLock {
	return &StripedLock{}
}

func fnvHash64(s string) uint64 {
	h := uint64(14695981039346656037)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= 1099511628211
	}
	return h
}

func (sl *StripedLock) Lock(key string) {
	stripe := fnvHash64(key) % NumStripes
	sl.locks[stripe].Lock()
}

func (sl *StripedLock) Unlock(key string) {
	stripe := fnvHash64(key) % NumStripes
	sl.locks[stripe].Unlock()
}

type BookRegistry struct {
	books   sync.Map
	stripes *StripedLock
}

func NewBookRegistry() *BookRegistry {
	return &BookRegistry{
		stripes: NewStripedLock(),
	}
}

func (br *BookRegistry) GetOrCreate(symbol string, factory func() *OrderBook) *OrderBook {
	if val, ok := br.books.Load(symbol); ok {
		return val.(*OrderBook)
	}

	br.stripes.Lock(symbol)
	defer br.stripes.Unlock(symbol)

	if val, ok := br.books.Load(symbol); ok {
		return val.(*OrderBook)
	}

	book := factory()
	br.books.Store(symbol, book)
	return book
}

func (br *BookRegistry) Get(symbol string) (*OrderBook, bool) {
	val, ok := br.books.Load(symbol)
	if !ok {
		return nil, false
	}
	return val.(*OrderBook), true
}

func (br *BookRegistry) Range(fn func(symbol string, book *OrderBook) bool) {
	br.books.Range(func(key, value interface{}) bool {
		return fn(key.(string), value.(*OrderBook))
	})
}

func (br *BookRegistry) StopAll() {
	br.books.Range(func(key, value interface{}) bool {
		value.(*OrderBook).Stop()
		return true
	})
}
