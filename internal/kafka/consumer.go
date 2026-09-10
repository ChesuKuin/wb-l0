package kafka

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"wb-l0/internal/cache"
	"wb-l0/internal/db"
	"wb-l0/internal/models"
)

const (
	maxRetries      = 5
	retryBaseDelay  = 500 * time.Millisecond
	deadLetterFile  = "dead_letter.log"
)

// RunConsumer подписывается на топик и в цикле читает сообщения.
// Для каждого сообщения:
//  1. парсим JSON — если не получилось, это не временный сбой, а мусорные
//     данные: логируем в dead-letter и коммитим, чтобы не застрять;
//  2. валидируем обязательные поля — по той же логике, что и парсинг;
//  3. сохраняем в БД внутри транзакции, с ограниченным числом retry
//     и backoff — партиция НЕ продвигается, пока сообщение не обработано;
//  4. кэш обновляем сразу после успешной транзакции — до коммита offset,
//     чтобы сбой коммита не оставил кэш с устаревшими данными;
//  5. коммитим offset только после успешной записи в БД. Если после всех
//     retry сохранить так и не удалось — сообщение уходит в dead-letter
//     и коммитится, чтобы один "вечно падающий" заказ не остановил
//     обработку всех остальных навсегда.
func RunConsumer(ctx context.Context, brokers []string, topic, groupID string, store *db.Store, c *cache.Cache) {
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: groupID,
	})
	defer reader.Close()

	log.Printf("[kafka] consumer запущен, топик=%s, брокеры=%v", topic, brokers)

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Println("[kafka] остановка consumer (контекст отменён)")
				return
			}
			log.Printf("[kafka] ошибка чтения сообщения: %v", err)
			continue
		}

		var order models.Order
		if err := json.Unmarshal(msg.Value, &order); err != nil {
			log.Printf("[kafka] невалидный JSON, сообщение в dead-letter: %v", err)
			writeDeadLetter(msg.Value, err)
			commitWithLog(ctx, reader, msg)
			continue
		}

		if err := order.Validate(); err != nil {
			log.Printf("[kafka] сообщение не прошло валидацию, в dead-letter: %v", err)
			writeDeadLetter(msg.Value, err)
			commitWithLog(ctx, reader, msg)
			continue
		}

		// Сообщение "хорошее" — партиция не должна продвинуться дальше,
		// пока мы либо не сохраним его, либо не исчерпаем retry.
		saved := false
		var lastErr error
		for attempt := 1; attempt <= maxRetries; attempt++ {
			if err := store.SaveOrder(ctx, order); err != nil {
				lastErr = err
				log.Printf("[kafka] попытка %d/%d сохранить заказ %s не удалась: %v",
					attempt, maxRetries, order.OrderUID, err)
				select {
				case <-time.After(retryBaseDelay * time.Duration(attempt)):
				case <-ctx.Done():
					return
				}
				continue
			}
			saved = true
			break
		}

		if !saved {
			log.Printf("[kafka] заказ %s не удалось сохранить после %d попыток, в dead-letter: %v",
				order.OrderUID, maxRetries, lastErr)
			writeDeadLetter(msg.Value, lastErr)
			commitWithLog(ctx, reader, msg)
			continue
		}

		// Кэш обновляем сразу после успешной транзакции, ДО коммита offset —
		// так сбой коммита не оставит в кэше устаревшую версию заказа.
		c.Set(order)

		commitWithLog(ctx, reader, msg)
		log.Printf("[kafka] заказ %s сохранён и закэширован", order.OrderUID)
	}
}

func commitWithLog(ctx context.Context, reader *kafkago.Reader, msg kafkago.Message) {
	if err := reader.CommitMessages(ctx, msg); err != nil {
		log.Printf("[kafka] ошибка коммита offset (partition=%d, offset=%d): %v",
			msg.Partition, msg.Offset, err)
	}
}

// writeDeadLetter дописывает необработанное сообщение в локальный файл,
// чтобы его можно было разобрать вручную позже, не теряя данные молча.
func writeDeadLetter(raw []byte, cause error) {
	f, err := os.OpenFile(deadLetterFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("[kafka] не удалось открыть dead-letter файл: %v", err)
		return
	}
	defer f.Close()

	entry := time.Now().Format(time.RFC3339) + " cause=" + safeErr(cause) + " payload=" + string(raw) + "\n"
	if _, err := f.WriteString(entry); err != nil {
		log.Printf("[kafka] не удалось записать в dead-letter файл: %v", err)
	}
}

func safeErr(err error) string {
	if err == nil {
		return "unknown"
	}
	return err.Error()
}
