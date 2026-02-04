package store_test

import (
	"testing"

	"github.com/holacloud/store"
	"github.com/holacloud/store/testutils"
)

func TestInMemory(t *testing.T) {

	p := store.NewStoreMemory[testutils.TestItem]()

	testutils.SuitePersistencer(p, t)
	testutils.SuiteOptimisticLocking(p, t)
}
