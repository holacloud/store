package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

type Identifier interface {
	GetId() string
	GetVersion() int64
	SetVersion(version int64)
}

var ErrVersionGone = errors.New("version gone")

type Storer[T Identifier] interface {
	// List returns a list of items.
	// If filters are provided, they act as key-value pairs to filter the results.
	// For example:
	// items, err := store.List(ctx, "user_id", "user-3343", "order_id", "order-35222")
	List(ctx context.Context, filters ...string) ([]*T, error)
	Put(ctx context.Context, item *T) error
	Get(ctx context.Context, id string) (*T, error)
	Delete(ctx context.Context, id string) error
}

func matchFilters(item any, filters []string) bool {
	if len(filters) == 0 {
		return true
	}
	b, err := json.Marshal(item)
	if err != nil {
		return false
	}
	var m map[string]interface{}
	if err := json.Unmarshal(b, &m); err != nil {
		return false
	}
	for i := 0; i < len(filters); i += 2 {
		val, ok := m[filters[i]]
		if !ok {
			return false
		}
		strVal := fmt.Sprintf("%v", val)
		if strVal != filters[i+1] {
			return false
		}
	}
	return true
}
