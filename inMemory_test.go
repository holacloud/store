package store_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/holacloud/store"
	"github.com/holacloud/store/testutils"
)

func TestInMemory(t *testing.T) {

	p := store.NewStoreMemory[testutils.TestItem]()

	testutils.SuitePersistencer(p, t)
	testutils.SuiteOptimisticLocking(p, t)
}

func BenchmarkInMemoryList(b *testing.B) {
	p := store.NewStoreMemory[testutils.TestItem]()
	ctx := context.Background()
	// Insert 100k items
	for i := 0; i < 10000; i++ {
		item := testutils.TestItem{Id: store.NewId(fmt.Sprintf("id%d", i))}
		_ = p.Put(ctx, &item)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		items, _ := p.List(ctx)
		count := 0
		for range items {
			count++
		}
		if count != 10000 {
			b.Fatalf("expected 10000 items, got %d", count)
		}
	}
}
