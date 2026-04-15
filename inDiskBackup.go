package store

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type backupAction uint8

const (
	backupActionPut backupAction = iota + 1
	backupActionDelete
)

type StoreDiskBackup[T Identifier] struct {
	primary Storer[T]
	backup  *StoreDisk[T]

	mutex      sync.Mutex
	pending    map[string]backupAction
	running    bool
	closed     bool
	lastErr    error
	wakeup     chan struct{}
	workerDone chan struct{}
}

func NewStoreDiskBackup[T Identifier](primary Storer[T], backupDir string) (*StoreDiskBackup[T], error) {
	if primary == nil {
		return nil, fmt.Errorf("primary store is required")
	}

	backup, err := NewStoreDisk[T](backupDir)
	if err != nil {
		return nil, err
	}

	result := &StoreDiskBackup[T]{
		primary:    primary,
		backup:     backup,
		pending:    map[string]backupAction{},
		wakeup:     make(chan struct{}, 1),
		workerDone: make(chan struct{}),
	}

	if err := result.syncAll(context.Background()); err != nil {
		return nil, err
	}

	go result.runBackupWorker()

	return result, nil
}

func (s *StoreDiskBackup[T]) List(ctx context.Context) ([]*T, error) {
	return s.primary.List(ctx)
}

func (s *StoreDiskBackup[T]) Put(ctx context.Context, item *T) error {
	if err := s.primary.Put(ctx, item); err != nil {
		return err
	}

	s.enqueue((*item).GetId(), backupActionPut)
	return nil
}

func (s *StoreDiskBackup[T]) Get(ctx context.Context, id string) (*T, error) {
	return s.primary.Get(ctx, id)
}

func (s *StoreDiskBackup[T]) Delete(ctx context.Context, id string) error {
	if err := s.primary.Delete(ctx, id); err != nil {
		return err
	}

	s.enqueue(id, backupActionDelete)
	return nil
}

func (s *StoreDiskBackup[T]) Sync(ctx context.Context) error {
	t := time.NewTicker(2 * time.Millisecond)
	defer t.Stop()

	for {
		s.mutex.Lock()
		isIdle := len(s.pending) == 0 && !s.running
		err := s.lastErr
		s.mutex.Unlock()

		if isIdle {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
}

func (s *StoreDiskBackup[T]) Close(ctx context.Context) error {
	s.mutex.Lock()
	if !s.closed {
		s.closed = true
	}
	s.mutex.Unlock()

	select {
	case s.wakeup <- struct{}{}:
	default:
	}

	select {
	case <-s.workerDone:
		s.mutex.Lock()
		err := s.lastErr
		s.mutex.Unlock()
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *StoreDiskBackup[T]) enqueue(id string, action backupAction) {
	s.mutex.Lock()
	if s.closed {
		s.mutex.Unlock()
		return
	}

	s.pending[id] = action
	s.mutex.Unlock()

	select {
	case s.wakeup <- struct{}{}:
	default:
	}
}

func (s *StoreDiskBackup[T]) runBackupWorker() {
	defer close(s.workerDone)

	for {
		<-s.wakeup

		for {
			batch, shouldStop := s.nextBatch()
			if len(batch) == 0 {
				if shouldStop {
					return
				}
				break
			}

			s.persistBatch(batch)
		}
	}
}

func (s *StoreDiskBackup[T]) syncAll(ctx context.Context) error {
	items, err := s.primary.List(ctx)
	if err != nil {
		return fmt.Errorf("initial backup list primary: %w", err)
	}

	primaryIds := make(map[string]struct{}, len(items))
	for _, item := range items {
		if item == nil {
			continue
		}

		id := (*item).GetId()
		primaryIds[id] = struct{}{}

		if err := s.backup.Put(ctx, item); err != nil {
			return fmt.Errorf("initial backup put '%s': %w", id, err)
		}
	}

	backupItems, err := s.backup.List(ctx)
	if err != nil {
		return fmt.Errorf("initial backup list disk: %w", err)
	}

	for _, backupItem := range backupItems {
		if backupItem == nil {
			continue
		}

		id := (*backupItem).GetId()
		if _, exists := primaryIds[id]; exists {
			continue
		}

		if err := s.backup.Delete(ctx, id); err != nil {
			return fmt.Errorf("initial backup delete stale '%s': %w", id, err)
		}
	}

	return nil
}

func (s *StoreDiskBackup[T]) nextBatch() (map[string]backupAction, bool) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if len(s.pending) == 0 {
		s.running = false
		return nil, s.closed
	}

	batch := s.pending
	s.pending = map[string]backupAction{}
	s.running = true

	return batch, false
}

func (s *StoreDiskBackup[T]) persistBatch(batch map[string]backupAction) {
	ctx := context.Background()

	for id, action := range batch {
		syncErr := s.syncItem(ctx, id, action)
		if syncErr == nil {
			continue
		}

		s.mutex.Lock()
		s.lastErr = syncErr
		s.mutex.Unlock()
	}
}

func (s *StoreDiskBackup[T]) syncItem(ctx context.Context, id string, action backupAction) error {
	if action == backupActionDelete {
		if err := s.backup.Delete(ctx, id); err != nil {
			return fmt.Errorf("backup delete '%s': %w", id, err)
		}
		return nil
	}

	item, err := s.primary.Get(ctx, id)
	if err != nil {
		return fmt.Errorf("backup get from primary '%s': %w", id, err)
	}

	if item == nil {
		if err := s.backup.Delete(ctx, id); err != nil {
			return fmt.Errorf("backup delete missing '%s': %w", id, err)
		}
		return nil
	}

	if err := s.backup.Put(ctx, item); err != nil {
		return fmt.Errorf("backup put '%s': %w", id, err)
	}

	return nil
}
