package db

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"wb-l0/internal/models"
)

type Store struct {
	pool *pgxpool.Pool
}

// Connect открывает пул соединений к Postgres.
// dsn пример: "postgres://wbuser:wbpass@localhost:5432/wb_orders?sslmode=disable"
func Connect(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("db connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("db ping: %w", err)
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() {
	s.pool.Close()
}

// SaveOrder сохраняет заказ в БД одной транзакцией:
// orders + deliveries + payments + items.
// Если заказ с таким order_uid уже есть — просто обновляем (upsert),
// чтобы повторное сообщение из Kafka не роняло сервис ошибкой.
func (s *Store) SaveOrder(ctx context.Context, o models.Order) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx) // если Commit уже был вызван, Rollback — no-op

	_, err = tx.Exec(ctx, `
		INSERT INTO orders (order_uid, track_number, entry, locale, internal_signature,
			customer_id, delivery_service, shardkey, sm_id, date_created, oof_shard)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
		ON CONFLICT (order_uid) DO UPDATE SET
			track_number = EXCLUDED.track_number,
			entry = EXCLUDED.entry,
			locale = EXCLUDED.locale,
			internal_signature = EXCLUDED.internal_signature,
			customer_id = EXCLUDED.customer_id,
			delivery_service = EXCLUDED.delivery_service,
			shardkey = EXCLUDED.shardkey,
			sm_id = EXCLUDED.sm_id,
			date_created = EXCLUDED.date_created,
			oof_shard = EXCLUDED.oof_shard
	`, o.OrderUID, o.TrackNumber, o.Entry, o.Locale, o.InternalSignature,
		o.CustomerID, o.DeliveryService, o.Shardkey, o.SmID, o.DateCreated, o.OofShard)
	if err != nil {
		return fmt.Errorf("insert order: %w", err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO deliveries (order_uid, name, phone, zip, city, address, region, email)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
		ON CONFLICT (order_uid) DO UPDATE SET
			name = EXCLUDED.name, phone = EXCLUDED.phone, zip = EXCLUDED.zip,
			city = EXCLUDED.city, address = EXCLUDED.address, region = EXCLUDED.region,
			email = EXCLUDED.email
	`, o.OrderUID, o.Delivery.Name, o.Delivery.Phone, o.Delivery.Zip, o.Delivery.City,
		o.Delivery.Address, o.Delivery.Region, o.Delivery.Email)
	if err != nil {
		return fmt.Errorf("insert delivery: %w", err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO payments (order_uid, transaction, request_id, currency, provider,
			amount, payment_dt, bank, delivery_cost, goods_total, custom_fee)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
		ON CONFLICT (order_uid) DO UPDATE SET
			transaction = EXCLUDED.transaction, request_id = EXCLUDED.request_id,
			currency = EXCLUDED.currency, provider = EXCLUDED.provider,
			amount = EXCLUDED.amount, payment_dt = EXCLUDED.payment_dt,
			bank = EXCLUDED.bank, delivery_cost = EXCLUDED.delivery_cost,
			goods_total = EXCLUDED.goods_total, custom_fee = EXCLUDED.custom_fee
	`, o.OrderUID, o.Payment.Transaction, o.Payment.RequestID, o.Payment.Currency,
		o.Payment.Provider, o.Payment.Amount, o.Payment.PaymentDt, o.Payment.Bank,
		o.Payment.DeliveryCost, o.Payment.GoodsTotal, o.Payment.CustomFee)
	if err != nil {
		return fmt.Errorf("insert payment: %w", err)
	}

	// Простой вариант: удаляем старые items и вставляем заново.
	// Для учебного задания этого достаточно и защищает от дублей при повторной обработке.
	_, err = tx.Exec(ctx, `DELETE FROM items WHERE order_uid = $1`, o.OrderUID)
	if err != nil {
		return fmt.Errorf("clear items: %w", err)
	}

	for _, it := range o.Items {
		_, err = tx.Exec(ctx, `
			INSERT INTO items (order_uid, chrt_id, track_number, price, rid, name,
				sale, size, total_price, nm_id, brand, status)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
		`, o.OrderUID, it.ChrtID, it.TrackNumber, it.Price, it.Rid, it.Name,
			it.Sale, it.Size, it.TotalPrice, it.NmID, it.Brand, it.Status)
		if err != nil {
			return fmt.Errorf("insert item: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	return nil
}

// GetOrder читает один заказ по order_uid из БД (используется, если его нет в кэше).
func (s *Store) GetOrder(ctx context.Context, orderUID string) (models.Order, error) {
	var o models.Order
	o.OrderUID = orderUID

	err := s.pool.QueryRow(ctx, `
		SELECT track_number, entry, locale, internal_signature, customer_id,
			delivery_service, shardkey, sm_id, date_created, oof_shard
		FROM orders WHERE order_uid = $1
	`, orderUID).Scan(&o.TrackNumber, &o.Entry, &o.Locale, &o.InternalSignature,
		&o.CustomerID, &o.DeliveryService, &o.Shardkey, &o.SmID, &o.DateCreated, &o.OofShard)
	if err != nil {
		if err == pgx.ErrNoRows {
			return o, fmt.Errorf("order not found: %s", orderUID)
		}
		return o, fmt.Errorf("get order: %w", err)
	}

	err = s.pool.QueryRow(ctx, `
		SELECT name, phone, zip, city, address, region, email
		FROM deliveries WHERE order_uid = $1
	`, orderUID).Scan(&o.Delivery.Name, &o.Delivery.Phone, &o.Delivery.Zip,
		&o.Delivery.City, &o.Delivery.Address, &o.Delivery.Region, &o.Delivery.Email)
	if err != nil && err != pgx.ErrNoRows {
		return o, fmt.Errorf("get delivery: %w", err)
	}

	err = s.pool.QueryRow(ctx, `
		SELECT transaction, request_id, currency, provider, amount, payment_dt,
			bank, delivery_cost, goods_total, custom_fee
		FROM payments WHERE order_uid = $1
	`, orderUID).Scan(&o.Payment.Transaction, &o.Payment.RequestID, &o.Payment.Currency,
		&o.Payment.Provider, &o.Payment.Amount, &o.Payment.PaymentDt, &o.Payment.Bank,
		&o.Payment.DeliveryCost, &o.Payment.GoodsTotal, &o.Payment.CustomFee)
	if err != nil && err != pgx.ErrNoRows {
		return o, fmt.Errorf("get payment: %w", err)
	}

	rows, err := s.pool.Query(ctx, `
		SELECT chrt_id, track_number, price, rid, name, sale, size, total_price, nm_id, brand, status
		FROM items WHERE order_uid = $1
	`, orderUID)
	if err != nil {
		return o, fmt.Errorf("get items: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var it models.Item
		if err := rows.Scan(&it.ChrtID, &it.TrackNumber, &it.Price, &it.Rid, &it.Name,
			&it.Sale, &it.Size, &it.TotalPrice, &it.NmID, &it.Brand, &it.Status); err != nil {
			return o, fmt.Errorf("scan item: %w", err)
		}
		o.Items = append(o.Items, it)
	}

	return o, nil
}

// GetAllOrderUIDs возвращает все id заказов — используется при старте сервиса,
// чтобы восстановить кэш из БД.
func (s *Store) GetAllOrderUIDs(ctx context.Context) ([]string, error) {
	rows, err := s.pool.Query(ctx, `SELECT order_uid FROM orders`)
	if err != nil {
		return nil, fmt.Errorf("get all order uids: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}
