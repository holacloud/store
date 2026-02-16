package store_test

import (
	"context"
	"testing"

	"github.com/fulldump/biff"
	"github.com/holacloud/store"
	"github.com/holacloud/store/testutils"
)

func TestStoreDisk(t *testing.T) {

	disk, err := store.NewStoreDisk[testutils.TestItem](t.TempDir())
	biff.AssertNil(err)

	p, err := store.NewStoreCached[testutils.TestItem](disk, nil)
	biff.AssertNil(err)

	testutils.SuitePersistencer(p, t)
	testutils.SuiteOptimisticLocking(p, t)
}

func TestStoreDisk_Load(t *testing.T) {
	dir := t.TempDir()

	p, err := store.NewStoreDisk[testutils.TestItem](dir)
	biff.AssertNil(err)

	err = p.Put(context.Background(), &testutils.TestItem{
		Id:    store.NewId("33"),
		Title: "test",
	})
	biff.AssertNil(err)

	// Test loading from disk into a new StoreCached
	disk2, err := store.NewStoreDisk[testutils.TestItem](dir)
	biff.AssertNil(err)

	p2, err := store.NewStoreCached[testutils.TestItem](disk2, nil)
	biff.AssertNil(err)

	result, err := p2.List(context.Background())
	biff.AssertNil(err)
	// fmt.Println(result) // remove print

	// Verify item is in the list
	biff.AssertEqual(len(result), 1)
	biff.AssertEqual(result[0].Title, "test")

	item, err := p2.Get(context.Background(), "33")
	biff.AssertNil(err)
	biff.AssertEqual(item.Title, "test")
}
