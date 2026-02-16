package store_test

import (
	"testing"

	"github.com/fulldump/biff"
	"github.com/holacloud/store"
	"github.com/holacloud/store/testutils"
)

func TestStoreCached(t *testing.T) {

	disk, err := store.NewStoreDisk[testutils.TestItem](t.TempDir())
	biff.AssertNil(err)

	p, err := store.NewStoreCached(disk, nil)
	biff.AssertNil(err)

	testutils.SuitePersistencer(p, t)
	testutils.SuiteOptimisticLocking(p, t)
}
