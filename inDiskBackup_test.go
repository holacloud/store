package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/fulldump/biff"
	"github.com/holacloud/store"
	"github.com/holacloud/store/testutils"
)

func TestStoreDiskBackup(t *testing.T) {
	dir := t.TempDir()

	primary := store.NewStoreMemory[testutils.TestItem]()
	p, err := store.NewStoreDiskBackup[testutils.TestItem](primary, dir)
	biff.AssertNil(err)

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = p.Close(ctx)
	})

	testutils.SuitePersistencer(p, t)
	testutils.SuiteOptimisticLocking(p, t)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err = p.Sync(ctx)
	biff.AssertNil(err)
}

func TestStoreDiskBackup_SyncsOnCreate(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	primary := store.NewStoreMemory[testutils.TestItem]()
	err := primary.Put(ctx, &testutils.TestItem{
		Id:    store.NewId("existing"),
		Title: "existing-title",
	})
	biff.AssertNil(err)

	preExistingBackup, err := store.NewStoreDisk[testutils.TestItem](dir)
	biff.AssertNil(err)
	err = preExistingBackup.Put(ctx, &testutils.TestItem{
		Id:    store.NewId("stale"),
		Title: "stale-title",
	})
	biff.AssertNil(err)

	p, err := store.NewStoreDiskBackup[testutils.TestItem](primary, dir)
	biff.AssertNil(err)

	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = p.Close(closeCtx)
	})

	backup, err := store.NewStoreDisk[testutils.TestItem](dir)
	biff.AssertNil(err)

	list, err := backup.List(ctx)
	biff.AssertNil(err)
	biff.AssertEqual(len(list), 1)
	biff.AssertEqual(list[0].GetId(), "existing")
	biff.AssertEqual(list[0].Title, "existing-title")
}

func TestStoreDiskBackup_PersistsLatestState(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	primary := store.NewStoreMemory[testutils.TestItem]()
	p, err := store.NewStoreDiskBackup[testutils.TestItem](primary, dir)
	biff.AssertNil(err)

	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = p.Close(closeCtx)
	})

	err = p.Put(ctx, &testutils.TestItem{
		Id:    store.NewId("1"),
		Title: "first",
	})
	biff.AssertNil(err)

	item1, err := p.Get(ctx, "1")
	biff.AssertNil(err)
	item1.Title = "first-updated"
	err = p.Put(ctx, item1)
	biff.AssertNil(err)

	err = p.Put(ctx, &testutils.TestItem{
		Id:    store.NewId("2"),
		Title: "to-delete",
	})
	biff.AssertNil(err)
	err = p.Delete(ctx, "2")
	biff.AssertNil(err)

	syncCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err = p.Sync(syncCtx)
	biff.AssertNil(err)

	backup, err := store.NewStoreDisk[testutils.TestItem](dir)
	biff.AssertNil(err)

	list, err := backup.List(ctx)
	biff.AssertNil(err)
	biff.AssertEqual(len(list), 1)
	biff.AssertEqual(list[0].GetId(), "1")
	biff.AssertEqual(list[0].Title, "first-updated")
}
