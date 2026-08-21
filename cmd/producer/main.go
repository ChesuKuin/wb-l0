package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	kafkago "github.com/segmentio/kafka-go"

	"wb-l0/internal/models"
)

// Небольшая программа-эмулятор: читает JSON-файл с заказом,
// подставляет новый order_uid (чтобы можно было слать много раз подряд)
// и отправляет его в Kafka.
//
// Использование:
//
//	go run ./cmd/producer -file ./data/model.json
//	go run ./cmd/producer -file ./data/model.json -bad     // отправит невалидный JSON (для проверки обработки ошибок)
func main() {
	filePath := flag.String("file", "./data/model.json", "путь к JSON-файлу с примером заказа")
	brokersFlag := flag.String("brokers", "localhost:9092", "адреса брокеров Kafka через запятую")
	topic := flag.String("topic", "orders", "имя топика")
	sendBad := flag.Bool("bad", false, "отправить намеренно битое сообщение (для проверки обработки ошибок)")
	flag.Parse()

	brokers := strings.Split(*brokersFlag, ",")

	writer := &kafkago.Writer{
		Addr:     kafkago.TCP(brokers...),
		Topic:    *topic,
		Balancer: &kafkago.LeastBytes{},
	}
	defer writer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if *sendBad {
		bad := []byte(`{"order_uid": "broken", "track_number": ` + "не json" + `}`)
		if err := writer.WriteMessages(ctx, kafkago.Message{Value: bad}); err != nil {
			log.Fatalf("ошибка отправки битого сообщения: %v", err)
		}
		log.Println("[producer] отправлено намеренно битое сообщение")
		return
	}

	raw, err := os.ReadFile(*filePath)
	if err != nil {
		log.Fatalf("не удалось прочитать файл %s: %v", *filePath, err)
	}

	var order models.Order
	if err := json.Unmarshal(raw, &order); err != nil {
		log.Fatalf("файл %s не является валидным JSON заказа: %v", *filePath, err)
	}

	// новый уникальный id, чтобы каждый запуск создавал "новый" заказ
	newUID := uuid.New().String()
	order.OrderUID = newUID
	order.Payment.Transaction = newUID
	if order.DateCreated.IsZero() {
		order.DateCreated = time.Now()
	}

	payload, err := json.Marshal(order)
	if err != nil {
		log.Fatalf("ошибка сериализации заказа: %v", err)
	}

	if err := writer.WriteMessages(ctx, kafkago.Message{
		Key:   []byte(order.OrderUID),
		Value: payload,
	}); err != nil {
		log.Fatalf("ошибка отправки в Kafka: %v", err)
	}

	log.Printf("[producer] заказ отправлен, order_uid = %s", order.OrderUID)
	log.Printf("[producer] проверить: http://localhost:8081/order/%s", order.OrderUID)
}
