package store

import (
	"context"
	"encoding/json"
	"sync"
)

type StoreMemory[T Identifier] struct {
	Items map[string]*T
	mutex sync.RWMutex
}

func NewStoreMemory[T Identifier]() *StoreMemory[T] {
	return &StoreMemory[T]{
		Items: map[string]*T{},
	}
}

func (f *StoreMemory[T]) List(ctx context.Context) ([]*T, error) {

	f.mutex.RLock()
	defer f.mutex.RUnlock()

	result := make([]*T, len(f.Items))

	i := -1
	for _, item := range f.Items {
		i++
		result[i], _ = f.Get(ctx, (*item).GetId())
	}

	return result, nil
}

func (f *StoreMemory[T]) Put(ctx context.Context, item *T) error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	if oldItem, ok := f.Items[(*item).GetId()]; ok {
		if (*oldItem).GetVersion() != (*item).GetVersion() {
			return ErrVersionGone
		}
	}

	(*item).SetVersion((*item).GetVersion() + 1)
	f.Items[(*item).GetId()] = item // todo: copy?
	return nil
}

func (f *StoreMemory[T]) Get(ctx context.Context, id string) (*T, error) {
	f.mutex.RLock()
	defer f.mutex.RUnlock()

	item, ok := f.Items[id]
	if !ok {
		return nil, nil
	}

	// Copy
	var newItem *T
	remarshal(item, &newItem)

	return newItem, nil
}

func remarshal(in, out any) {
	b, _ := json.Marshal(in)
	_ = json.Unmarshal(b, &out)
}

func (f *StoreMemory[T]) Delete(ctx context.Context, id string) error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	delete(f.Items, id)
	return nil
}
