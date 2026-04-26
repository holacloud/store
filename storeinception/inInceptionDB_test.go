package storeinception

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/holacloud/store/testutils"
)

func TestInInception(t *testing.T) {

	collection := "testing-" + uuid.NewString()
	var p *StoreInception[testutils.TestItem]

	for _, base := range []string{"https://inceptiondb.io/v1", "http://inceptiondb:1212/v1", "http://localhost:1212/v1"} {
		p = New[testutils.TestItem](&ConfigInceptionDB{
			Base:       base,
			Collection: collection,
		})
		items, err := p.List(context.Background())
		if err == nil {
			for range items {
			}
			break
		}
	}

	testutils.SuitePersistencer(p, t)
	testutils.SuiteOptimisticLocking(p, t) // working on this!
}
