package cache

import (
	"container/list"
	"sync"

	"wb-l0/internal/models"
)

// DefaultMaxSize — сколько заказов держим в памяти одновременно.
// При превышении вытесняется наименее недавно использованный (LRU).
const DefaultMaxSize = 10_000

type entry struct {
	orderUID string
	order    models.Order
}

// Cache — потокобезопасный LRU-кэш заказов ограниченного размера.
// Без лимита map росла бы бесконечно вместе с числом заказов и рано
// или поздно уронила бы процесс по нехватке памяти.
type Cache struct {
	mu      sync.RWMutex
	maxSize int
	items   map[string]*list.Element // order_uid -> элемент списка
	order   *list.List               // список entry, голова — самый свежий
}

func New() *Cache {
	return NewWithSize(DefaultMaxSize)
}

func NewWithSize(maxSize int) *Cache {
	if maxSize <= 0 {
		maxSize = DefaultMaxSize
	}
	return &Cache{
		maxSize: maxSize,
		items:   make(map[string]*list.Element),
		order:   list.New(),
	}
}

func (c *Cache) Set(o models.Order) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if el, ok := c.items[o.OrderUID]; ok {
		el.Value.(*entry).order = o
		c.order.MoveToFront(el)
		return
	}

	el := c.order.PushFront(&entry{orderUID: o.OrderUID, order: o})
	c.items[o.OrderUID] = el

	if c.order.Len() > c.maxSize {
		oldest := c.order.Back()
		if oldest != nil {
			c.order.Remove(oldest)
			delete(c.items, oldest.Value.(*entry).orderUID)
		}
	}
}

func (c *Cache) Get(orderUID string) (models.Order, bool) {
	c.mu.Lock() // Lock, а не RLock — Get двигает элемент в LRU-списке
	defer c.mu.Unlock()

	el, ok := c.items[orderUID]
	if !ok {
		return models.Order{}, false
	}
	c.order.MoveToFront(el)
	return el.Value.(*entry).order, true
}

func (c *Cache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.order.Len()
}
