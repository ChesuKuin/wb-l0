package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

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

func getEnvInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func main() {
	// --- конфигурация через переменные окружения, с разумными дефолтами ---
	dsn := getEnv("DATABASE_DSN", "postgres://wbuser:wbpass@localhost:5432/wb_orders?sslmode=disable")
	kafkaBrokers := strings.Split(getEnv("KAFKA_BROKERS", "localhost:9092"), ",")
	kafkaTopic := getEnv("KAFKA_TOPIC", "orders")
	kafkaGroup := getEnv("KAFKA_GROUP", "orders-service")
	httpAddr := getEnv("HTTP_ADDR", ":8081")
	cacheMaxSize := getEnvInt("CACHE_MAX_SIZE", cache.DefaultMaxSize)
	warmupLimit := getEnvInt("CACHE_WARMUP_LIMIT", 1000)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// --- подключение к БД ---
	store, err := db.Connect(ctx, dsn)
	if err != nil {
		log.Fatalf("не удалось подключиться к БД: %v", err)
	}
	defer store.Close()
	log.Println("[service] подключение к Postgres установлено")

	// --- прогрев кэша: ограниченным батчем, не всей историей заказов.
	// Так старт сервиса не зависит от того, сколько всего заказов накопилось
	// в базе, и не рискует упереться в память при большой истории.
	c := cache.NewWithSize(cacheMaxSize)
	recent, err := store.GetRecentOrders(ctx, warmupLimit)
	if err != nil {
		log.Printf("[service] не удалось прогреть кэш из БД: %v", err)
	} else {
		for _, order := range recent {
			c.Set(order)
		}
		log.Printf("[service] кэш прогрет из БД, заказов: %d (лимит %d)", c.Len(), warmupLimit)
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

		// в кэше нет — пробуем достать из БД.
		// Различаем "заказа действительно нет" (404) и инфраструктурный
		// сбой БД (таймаут, обрыв соединения) — иначе при недоступности
		// Postgres клиент получал бы ложный окончательный "не найдено"
		// вместо сигнала повторить запрос.
		order, err := store.GetOrder(r.Context(), orderUID)
		if err != nil {
			if errors.Is(err, db.ErrOrderNotFound) {
				http.Error(w, `{"error":"заказ не найден"}`, http.StatusNotFound)
				return
			}
			log.Printf("[http] ошибка получения заказа %s из БД: %v", orderUID, err)
			http.Error(w, `{"error":"внутренняя ошибка, попробуйте повторить запрос"}`, http.StatusServiceUnavailable)
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

	// Таймауты обязательны: без них медленный или зависший клиент может
	// удерживать горутину и файловый дескриптор бесконечно, что при
	// доступности порта недоверенным клиентам ведёт к исчерпанию ресурсов.
	srv := &http.Server{
		Addr:              httpAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	go func() {
		log.Printf("[service] HTTP сервер слушает на %s", httpAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("[service] получен сигнал остановки, завершаем работу...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
}
