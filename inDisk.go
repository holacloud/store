package store

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
)

type StoreDisk[T Identifier] struct {
	dataDir string
	cache   *StoreMemory[T]
}

func NewStoreDisk[T Identifier](dataDir string) (*StoreDisk[T], error) {

	// ensure dir
	err := os.MkdirAll(dataDir, 0777)
	if err != nil {
		return nil, fmt.Errorf("ERROR: ensure data dir '%s': %s\n", dataDir, err.Error())
	}

	cache := NewStoreMemory[T]()

	// load dir
	err = filepath.WalkDir(dataDir, func(filename string, d fs.DirEntry, walkErr error) error {

		if walkErr != nil {
			return nil
		}

		if d.IsDir() {
			return nil
		}

		if !strings.EqualFold(".json", path.Ext(filename)) {
			return nil
		}

		f, err := os.Open(filename)
		if err != nil {
			log.Printf("error loading '%s': %s\n", filename, err.Error())
			return nil // todo. check if err should be returned
		}

		var item *T
		err = json.NewDecoder(f).Decode(&item)
		if err != nil {
			log.Printf("error decoding '%s': %s\n", filename, err.Error())
			return nil // todo. check if err should be returned
		}

		return cache.Put(context.Background(), item)
	})
	if err != nil {
		return nil, fmt.Errorf("load items: %s", err.Error())
	}

	return &StoreDisk[T]{
		dataDir: dataDir,
		cache:   cache,
	}, nil
}

func (f *StoreDisk[T]) List(ctx context.Context) ([]*T, error) {
	return f.cache.List(ctx)
}

func (f *StoreDisk[T]) Put(ctx context.Context, item *T) error {

	filename := path.Join(f.dataDir, (*item).GetId()+".json")

	fd, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer fd.Close()

	e := json.NewEncoder(fd)
	e.SetIndent("", "    ")
	err = e.Encode(item)
	if err != nil {
		return fmt.Errorf("persisting %s: %s\n", filename, err.Error())
	}

	return f.cache.Put(ctx, item)
}

func (f *StoreDisk[T]) Get(ctx context.Context, id string) (*T, error) {
	return f.cache.Get(ctx, id)
}

func (f *StoreDisk[T]) Delete(ctx context.Context, id string) error {

	item, err := f.Get(ctx, id)
	if err != nil {
		return fmt.Errorf("item '%s' does not exist", id)
	}

	filename := path.Join(f.dataDir, (*item).GetId()+".json")
	err = os.Remove(filename)
	if err != nil {
		return fmt.Errorf("item '%s' persistence error: %s", id, err.Error())
	}

	return f.cache.Delete(ctx, id)
}
