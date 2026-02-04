package store_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/fulldump/biff"
	"github.com/holacloud/store"
	"github.com/holacloud/store/testutils"
)

func TestStoreDisk(t *testing.T) {

	p, err := store.NewStoreDisk[testutils.TestItem](t.TempDir())

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

	p2, err := store.NewStoreDisk[testutils.TestItem](dir)
	biff.AssertNil(err)
	_ = p2

	result, err := p2.List(context.Background())
	biff.AssertNil(err)
	fmt.Println(result)

	item, err := p2.Get(context.Background(), "33")
	biff.AssertNil(err)
	biff.AssertEqual(item.Title, "test")
}
