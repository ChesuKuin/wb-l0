package kafka

import (
	"context"
	"encoding/json"
	"log"

	kafkago "github.com/segmentio/kafka-go"

	"wb-l0/internal/cache"
	"wb-l0/internal/db"
	"wb-l0/internal/models"
)

// RunConsumer подписывается на топик и в бесконечном цикле читает сообщения.
// Для каждого сообщения:
//  1. пытаемся распарсить JSON — если не получилось, логируем и пропускаем
//     (не коммитим смещение, но и не падаем);
//  2. валидируем обязательные поля;
//  3. сохраняем в БД внутри транзакции;
//  4. только после успешной записи в БД коммитим сообщение (offset);
//  5. кладём заказ в кэш.
func RunConsumer(ctx context.Context, brokers []string, topic, groupID string, store *db.Store, c *cache.Cache) {
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: groupID,
		// GroupID включает механизм consumer group + ручной коммит смещений через CommitMessages
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
			log.Printf("[kafka] невалидный JSON, сообщение пропущено: %v", err)
			// коммитим, чтобы не зациклиться на одном и том же битом сообщении
			_ = reader.CommitMessages(ctx, msg)
			continue
		}

		if err := order.Validate(); err != nil {
			log.Printf("[kafka] сообщение не прошло валидацию, пропущено: %v", err)
			_ = reader.CommitMessages(ctx, msg)
			continue
		}

		if err := store.SaveOrder(ctx, order); err != nil {
			log.Printf("[kafka] ошибка сохранения в БД, offset НЕ коммитим, попробуем позже: %v", err)
			// не коммитим — сообщение будет прочитано снова после перезапуска/на след. итерации
			continue
		}

		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("[kafka] ошибка коммита offset: %v", err)
			continue
		}

		c.Set(order)
		log.Printf("[kafka] заказ %s сохранён и закэширован", order.OrderUID)
	}
}
