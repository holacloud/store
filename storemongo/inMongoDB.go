package storemongo

import (
	"context"
	"time"

	"github.com/holacloud/store"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/mongo/driver/connstring"
)

type StoreMongo[T store.Identifier] struct {
	collectionName string
	connection     string
	client         *mongo.Client
	database       *mongo.Database
}

func New[T store.Identifier](collectionName, connection string) (*StoreMongo[T], error) {

	cs, err := connstring.ParseAndValidate(connection)
	if err != nil {
		return nil, err
	}

	databaseName := cs.Database
	if databaseName == "" {
		databaseName = "itemstest"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(connection))
	if err != nil {
		return nil, err
	}

	// ensure unique "id" index for items :D
	database := client.Database(databaseName)
	// _, err = database.Collection(collectionName).Indexes().CreateOne(context.Background(), mongo.IndexModel{
	// 	Keys: bson.M{"id": 1},
	// })
	if err != nil {
		return nil, err
	}

	return &StoreMongo[T]{
		collectionName: collectionName,
		connection:     connection,
		client:         client, // might not be needed
		database:       database,
	}, nil
}

func (f *StoreMongo[T]) List(ctx context.Context) ([]*T, error) {

	cur, err := f.database.Collection(f.collectionName).Find(ctx, bson.M{})
	if err != nil {
		return nil, err
	}
	defer cur.Close(context.Background())

	result := []*T{}

	for cur.Next(context.Background()) {
		var item *T
		err := cur.Decode(&item)
		if err != nil {
			return nil, err
		}
		result = append(result, item)
	}

	return result, nil
}

func (f *StoreMongo[T]) Put(ctx context.Context, item *T) error {
	filter := bson.M{
		"_id": (*item).GetId(),
	}
	version := (*item).GetVersion()
	if version > 0 {
		filter["version"] = version
	}
	set := *item
	set.SetVersion(version + 1)
	update := bson.M{
		"$set": set,
	}

	result, err := f.database.Collection(f.collectionName).UpdateOne(ctx, filter, update, options.Update().SetUpsert(true))

	if mongo.IsDuplicateKeyError(err) {
		return store.ErrVersionGone
	}

	if err != nil {
		return err
	}

	if result.MatchedCount == 0 && result.UpsertedCount == 0 {
		return store.ErrVersionGone
	}

	return nil
}

func (f *StoreMongo[T]) Get(ctx context.Context, id string) (*T, error) {
	var result *T
	err := f.database.Collection(f.collectionName).FindOne(ctx, bson.M{"_id": id}).Decode(&result)
	if err == mongo.ErrNoDocuments {
		return nil, nil
	}
	return result, err
}

func (f *StoreMongo[T]) Delete(ctx context.Context, id string) error {
	_, err := f.database.Collection(f.collectionName).DeleteOne(ctx, bson.M{"_id": id})
	return err
}
