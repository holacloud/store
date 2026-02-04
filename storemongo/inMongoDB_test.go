package storemongo

import (
	"context"
	"testing"
	"time"

	"github.com/fulldump/biff"
	"github.com/google/uuid"
	"github.com/holacloud/store/testutils"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func TestMongodb(t *testing.T) {

	dbname := "testing-" + uuid.New().String()
	connection := ""
	for _, c := range []string{"mongodb://mongodb:27017", "mongodb://localhost:27017"} {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		client, err := mongo.Connect(ctx, options.Client().ApplyURI(c))
		if err != nil {
			t.Log(c, err.Error())
			continue
		}

		err = client.Ping(ctx, nil)
		if err != nil {
			t.Log(c, err.Error())
			continue
		}

		defer func() {
			err := client.Database(dbname).Drop(context.Background())
			if err != nil {
				t.Log(err.Error())
			}
		}()

		connection = c
		break
	}

	if connection == "" {
		t.Skipf("MongoDB not available")
		return
	}

	connection += "/" + dbname
	t.Logf("Using connection: '%s'", connection)

	p, err := New[testutils.TestItem]("test_items", connection)
	biff.AssertNil(err)

	testutils.SuitePersistencer(p, t)
	testutils.SuiteOptimisticLocking(p, t)
}
