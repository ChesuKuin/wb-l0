package db

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"wb-l0/internal/models"
)

// ErrOrderNotFound — отличимая ошибка "заказа нет", в отличие от
// инфраструктурных сбоев (таймаут, обрыв соединения и т.п.).
// HTTP-хендлер должен превращать в 404 только её, а не любую ошибку БД.
var ErrOrderNotFound = errors.New("order not found")

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
// Если заказ с таким order_uid уже есть — обновляем (upsert),
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

	// Удаляем старые items и вставляем заново — защищает от дублей
	// при повторной обработке одного и того же order_uid.
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
// Все 4 запроса выполняются в одной read-only транзакции с уровнем изоляции
// Repeatable Read — это даёт согласованный снимок данных, даже если
// конкурентно идёт SaveOrder того же заказа (иначе можно было бы прочитать
// смесь старой и новой версии).
func (s *Store) GetOrder(ctx context.Context, orderUID string) (models.Order, error) {
	var o models.Order
	o.OrderUID = orderUID

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return o, fmt.Errorf("begin read tx: %w", err)
	}
	defer tx.Rollback(ctx)

	err = tx.QueryRow(ctx, `
		SELECT track_number, entry, locale, internal_signature, customer_id,
			delivery_service, shardkey, sm_id, date_created, oof_shard
		FROM orders WHERE order_uid = $1
	`, orderUID).Scan(&o.TrackNumber, &o.Entry, &o.Locale, &o.InternalSignature,
		&o.CustomerID, &o.DeliveryService, &o.Shardkey, &o.SmID, &o.DateCreated, &o.OofShard)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return o, ErrOrderNotFound
		}
		return o, fmt.Errorf("get order: %w", err)
	}

	err = tx.QueryRow(ctx, `
		SELECT name, phone, zip, city, address, region, email
		FROM deliveries WHERE order_uid = $1
	`, orderUID).Scan(&o.Delivery.Name, &o.Delivery.Phone, &o.Delivery.Zip,
		&o.Delivery.City, &o.Delivery.Address, &o.Delivery.Region, &o.Delivery.Email)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return o, fmt.Errorf("get delivery: %w", err)
	}

	err = tx.QueryRow(ctx, `
		SELECT transaction, request_id, currency, provider, amount, payment_dt,
			bank, delivery_cost, goods_total, custom_fee
		FROM payments WHERE order_uid = $1
	`, orderUID).Scan(&o.Payment.Transaction, &o.Payment.RequestID, &o.Payment.Currency,
		&o.Payment.Provider, &o.Payment.Amount, &o.Payment.PaymentDt, &o.Payment.Bank,
		&o.Payment.DeliveryCost, &o.Payment.GoodsTotal, &o.Payment.CustomFee)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return o, fmt.Errorf("get payment: %w", err)
	}

	rows, err := tx.Query(ctx, `
		SELECT chrt_id, track_number, price, rid, name, sale, size, total_price, nm_id, brand, status
		FROM items WHERE order_uid = $1
	`, orderUID)
	if err != nil {
		return o, fmt.Errorf("get items: %w", err)
	}
	for rows.Next() {
		var it models.Item
		if err := rows.Scan(&it.ChrtID, &it.TrackNumber, &it.Price, &it.Rid, &it.Name,
			&it.Sale, &it.Size, &it.TotalPrice, &it.NmID, &it.Brand, &it.Status); err != nil {
			rows.Close()
			return o, fmt.Errorf("scan item: %w", err)
		}
		o.Items = append(o.Items, it)
	}
	// rows.Err() обязателен: rows.Next() возвращает false и при обычном
	// конце данных, и при обрыве соединения/ошибке протокола на середине
	// чтения — без этой проверки такой сбой выглядел бы как "заказ без товаров".
	if err := rows.Err(); err != nil {
		rows.Close()
		return o, fmt.Errorf("read items rows: %w", err)
	}
	rows.Close()

	if err := tx.Commit(ctx); err != nil {
		return o, fmt.Errorf("commit read tx: %w", err)
	}

	return o, nil
}

// GetRecentOrders прогревает кэш одним-двумя запросами вместо 4N+1:
// один JOIN на orders+deliveries+payments, ограниченный limit, и один
// запрос items по всем выбранным order_uid сразу.
func (s *Store) GetRecentOrders(ctx context.Context, limit int) ([]models.Order, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT o.order_uid, o.track_number, o.entry, o.locale, o.internal_signature,
			o.customer_id, o.delivery_service, o.shardkey, o.sm_id, o.date_created, o.oof_shard,
			d.name, d.phone, d.zip, d.city, d.address, d.region, d.email,
			p.transaction, p.request_id, p.currency, p.provider, p.amount, p.payment_dt,
			p.bank, p.delivery_cost, p.goods_total, p.custom_fee
		FROM orders o
		LEFT JOIN deliveries d ON d.order_uid = o.order_uid
		LEFT JOIN payments p ON p.order_uid = o.order_uid
		ORDER BY o.date_created DESC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("get recent orders: %w", err)
	}

	byUID := make(map[string]*models.Order)
	var order []string // сохраняем порядок для стабильного вывода

	for rows.Next() {
		var o models.Order
		if err := rows.Scan(&o.OrderUID, &o.TrackNumber, &o.Entry, &o.Locale, &o.InternalSignature,
			&o.CustomerID, &o.DeliveryService, &o.Shardkey, &o.SmID, &o.DateCreated, &o.OofShard,
			&o.Delivery.Name, &o.Delivery.Phone, &o.Delivery.Zip, &o.Delivery.City,
			&o.Delivery.Address, &o.Delivery.Region, &o.Delivery.Email,
			&o.Payment.Transaction, &o.Payment.RequestID, &o.Payment.Currency, &o.Payment.Provider,
			&o.Payment.Amount, &o.Payment.PaymentDt, &o.Payment.Bank, &o.Payment.DeliveryCost,
			&o.Payment.GoodsTotal, &o.Payment.CustomFee,
		); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan recent order: %w", err)
		}
		byUID[o.OrderUID] = &o
		order = append(order, o.OrderUID)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("read recent orders rows: %w", err)
	}
	rows.Close()

	if len(order) == 0 {
		return nil, nil
	}

	itemRows, err := s.pool.Query(ctx, `
		SELECT order_uid, chrt_id, track_number, price, rid, name, sale, size, total_price, nm_id, brand, status
		FROM items WHERE order_uid = ANY($1)
	`, order)
	if err != nil {
		return nil, fmt.Errorf("get items for recent orders: %w", err)
	}
	for itemRows.Next() {
		var uid string
		var it models.Item
		if err := itemRows.Scan(&uid, &it.ChrtID, &it.TrackNumber, &it.Price, &it.Rid, &it.Name,
			&it.Sale, &it.Size, &it.TotalPrice, &it.NmID, &it.Brand, &it.Status); err != nil {
			itemRows.Close()
			return nil, fmt.Errorf("scan item for recent orders: %w", err)
		}
		if o, ok := byUID[uid]; ok {
			o.Items = append(o.Items, it)
		}
	}
	if err := itemRows.Err(); err != nil {
		itemRows.Close()
		return nil, fmt.Errorf("read items rows for recent orders: %w", err)
	}
	itemRows.Close()

	result := make([]models.Order, 0, len(order))
	for _, uid := range order {
		result = append(result, *byUID[uid])
	}
	return result, nil
}
