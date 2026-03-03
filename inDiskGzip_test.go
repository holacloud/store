package store_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/fulldump/biff"
	"github.com/holacloud/store"
	"github.com/holacloud/store/testutils"
)

func TestStoreDiskGzip(t *testing.T) {
	disk, err := store.NewStoreDiskGzip[testutils.TestItem](t.TempDir())
	biff.AssertNil(err)

	p, err := store.NewStoreCached[testutils.TestItem](disk, nil)
	biff.AssertNil(err)

	testutils.SuitePersistencer(p, t)
	testutils.SuiteOptimisticLocking(p, t)
}

func TestStoreDiskGzip_Load(t *testing.T) {
	dir := t.TempDir()

	p, err := store.NewStoreDiskGzip[testutils.TestItem](dir)
	biff.AssertNil(err)

	err = p.Put(context.Background(), &testutils.TestItem{
		Id:    store.NewId("33"),
		Title: "test-gzip",
	})
	biff.AssertNil(err)

	_, err = os.Stat(filepath.Join(dir, "33.json.gz"))
	biff.AssertNil(err)

	disk2, err := store.NewStoreDiskGzip[testutils.TestItem](dir)
	biff.AssertNil(err)

	p2, err := store.NewStoreCached[testutils.TestItem](disk2, nil)
	biff.AssertNil(err)

	result, err := p2.List(context.Background())
	biff.AssertNil(err)
	biff.AssertEqual(len(result), 1)
	biff.AssertEqual(result[0].Title, "test-gzip")

	item, err := p2.Get(context.Background(), "33")
	biff.AssertNil(err)
	biff.AssertEqual(item.Title, "test-gzip")
}

func TestStoreDiskGzip_NotRetroCompatibleWithJSON(t *testing.T) {
	dir := t.TempDir()

	plain, err := store.NewStoreDisk[testutils.TestItem](dir)
	biff.AssertNil(err)
	err = plain.Put(context.Background(), &testutils.TestItem{
		Id:    store.NewId("legacy-1"),
		Title: "legacy-json",
	})
	biff.AssertNil(err)

	gzipStore, err := store.NewStoreDiskGzip[testutils.TestItem](dir)
	biff.AssertNil(err)

	item, err := gzipStore.Get(context.Background(), "legacy-1")
	biff.AssertNil(err)
	biff.AssertNil(item)

	list, err := gzipStore.List(context.Background())
	biff.AssertNil(err)
	biff.AssertEqual(len(list), 0)
}
