package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/redis/go-redis/v9"

	"hft-fix-matching/matching"
)

type RedisTradeWriter struct {
	client *redis.Client
	ctx    context.Context
}

type TradeRecord struct {
	TradeID     string  `json:"trade_id"`
	Symbol      string  `json:"symbol"`
	Price       float64 `json:"price"`
	Quantity    int64   `json:"quantity"`
	BuyOrderID  string  `json:"buy_order_id"`
	SellOrderID string  `json:"sell_order_id"`
	Timestamp   int64   `json:"timestamp"`
}

func NewRedisTradeWriter(addr, password string, db int) *RedisTradeWriter {
	client := redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     password,
		DB:           db,
		PoolSize:     50,
		MinIdleConns: 10,
		DialTimeout:  0,
		ReadTimeout:  0,
		WriteTimeout: 0,
		PoolTimeout:  0,
	})

	ctx := context.Background()
	_, err := client.Ping(ctx).Result()
	if err != nil {
		log.Printf("[Redis] Connection failed: %v (will retry on write)", err)
	}

	return &RedisTradeWriter{
		client: client,
		ctx:    ctx,
	}
}

func (w *RedisTradeWriter) WriteTrade(trade *matching.Trade) error {
	record := TradeRecord{
		TradeID:     trade.TradeID,
		Symbol:      trade.Symbol,
		Price:       trade.Price,
		Quantity:    trade.Quantity,
		BuyOrderID:  trade.BuyOrderID,
		SellOrderID: trade.SellOrderID,
		Timestamp:   trade.Timestamp,
	}

	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshal trade: %w", err)
	}

	tradeKey := fmt.Sprintf("trade:%s", trade.TradeID)
	symbolKey := fmt.Sprintf("trades:%s", trade.Symbol)

	pipe := w.client.Pipeline()
	pipe.Set(w.ctx, tradeKey, data, 0)
	pipe.RPush(w.ctx, symbolKey, data)
	pipe.IncrBy(w.ctx, fmt.Sprintf("volume:%s", trade.Symbol), trade.Quantity)
	pipe.Incr(w.ctx, fmt.Sprintf("count:%s", trade.Symbol))

	_, err = pipe.Exec(w.ctx)
	if err != nil {
		return fmt.Errorf("redis pipeline exec: %w", err)
	}

	return nil
}

func (w *RedisTradeWriter) WriteTradesBatch(trades []*matching.Trade) error {
	if len(trades) == 0 {
		return nil
	}

	pipe := w.client.Pipeline()

	for _, trade := range trades {
		record := TradeRecord{
			TradeID:     trade.TradeID,
			Symbol:      trade.Symbol,
			Price:       trade.Price,
			Quantity:    trade.Quantity,
			BuyOrderID:  trade.BuyOrderID,
			SellOrderID: trade.SellOrderID,
			Timestamp:   trade.Timestamp,
		}

		data, err := json.Marshal(record)
		if err != nil {
			log.Printf("[Redis] Marshal error for trade %s: %v", trade.TradeID, err)
			continue
		}

		tradeKey := fmt.Sprintf("trade:%s", trade.TradeID)
		symbolKey := fmt.Sprintf("trades:%s", trade.Symbol)

		pipe.Set(w.ctx, tradeKey, data, 0)
		pipe.RPush(w.ctx, symbolKey, data)
		pipe.IncrBy(w.ctx, fmt.Sprintf("volume:%s", trade.Symbol), trade.Quantity)
	}

	_, err := pipe.Exec(w.ctx)
	if err != nil {
		return fmt.Errorf("redis batch exec: %w", err)
	}

	return nil
}

func (w *RedisTradeWriter) GetTrades(symbol string, count int64) ([]*TradeRecord, error) {
	symbolKey := fmt.Sprintf("trades:%s", symbol)
	results, err := w.client.LRange(w.ctx, symbolKey, -count, -1).Result()
	if err != nil {
		return nil, fmt.Errorf("redis lrange: %w", err)
	}

	var trades []*TradeRecord
	for _, result := range results {
		var record TradeRecord
		if err := json.Unmarshal([]byte(result), &record); err != nil {
			continue
		}
		trades = append(trades, &record)
	}
	return trades, nil
}

func (w *RedisTradeWriter) Close() error {
	return w.client.Close()
}
