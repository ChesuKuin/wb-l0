package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"wb-l0/internal/cache"
	"wb-l0/internal/db"
	"wb-l0/internal/kafka"
)

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	// --- конфигурация через переменные окружения, с разумными дефолтами ---
	dsn := getEnv("DATABASE_DSN", "postgres://wbuser:wbpass@localhost:5432/wb_orders?sslmode=disable")
	kafkaBrokers := strings.Split(getEnv("KAFKA_BROKERS", "localhost:9092"), ",")
	kafkaTopic := getEnv("KAFKA_TOPIC", "orders")
	kafkaGroup := getEnv("KAFKA_GROUP", "orders-service")
	httpAddr := getEnv("HTTP_ADDR", ":8081")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// --- подключение к БД ---
	store, err := db.Connect(ctx, dsn)
	if err != nil {
		log.Fatalf("не удалось подключиться к БД: %v", err)
	}
	defer store.Close()
	log.Println("[service] подключение к Postgres установлено")

	// --- восстановление кэша из БД при старте ---
	c := cache.New()
	ids, err := store.GetAllOrderUIDs(ctx)
	if err != nil {
		log.Printf("[service] не удалось получить список заказов для прогрева кэша: %v", err)
	} else {
		for _, id := range ids {
			order, err := store.GetOrder(ctx, id)
			if err != nil {
				log.Printf("[service] не удалось загрузить заказ %s в кэш: %v", id, err)
				continue
			}
			c.Set(order)
		}
		log.Printf("[service] кэш восстановлен из БД, заказов: %d", c.Len())
	}

	// --- запуск consumer'а Kafka в отдельной горутине ---
	go kafka.RunConsumer(ctx, kafkaBrokers, kafkaTopic, kafkaGroup, store, c)

	// --- HTTP API ---
	mux := http.NewServeMux()

	mux.HandleFunc("/order/", func(w http.ResponseWriter, r *http.Request) {
		orderUID := strings.TrimPrefix(r.URL.Path, "/order/")
		if orderUID == "" {
			http.Error(w, `{"error":"не указан order_uid"}`, http.StatusBadRequest)
			return
		}

		if order, ok := c.Get(orderUID); ok {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Cache", "HIT")
			json.NewEncoder(w).Encode(order)
			return
		}

		// в кэше нет — пробуем достать из БД
		order, err := store.GetOrder(r.Context(), orderUID)
		if err != nil {
			http.Error(w, `{"error":"заказ не найден"}`, http.StatusNotFound)
			return
		}
		c.Set(order) // подкладываем в кэш на будущее
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Cache", "MISS")
		json.NewEncoder(w).Encode(order)
	})

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	// статика фронтенда
	mux.Handle("/", http.FileServer(http.Dir("./web")))

	srv := &http.Server{
		Addr:    httpAddr,
		Handler: mux,
	}

	go func() {
		log.Printf("[service] HTTP сервер слушает на %s", httpAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("[service] получен сигнал остановки, завершаем работу...")
	_ = srv.Close()
}
