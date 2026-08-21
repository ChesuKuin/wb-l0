package cache

import (
	"sync"

	"wb-l0/internal/models"
)

// Cache — простое потокобезопасное хранилище заказов в памяти.
type Cache struct {
	mu   sync.RWMutex
	data map[string]models.Order
}

func New() *Cache {
	return &Cache{
		data: make(map[string]models.Order),
	}
}

func (c *Cache) Set(order models.Order) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.data[order.OrderUID] = order
}

func (c *Cache) Get(orderUID string) (models.Order, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	o, ok := c.data[orderUID]
	return o, ok
}

func (c *Cache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.data)
}
