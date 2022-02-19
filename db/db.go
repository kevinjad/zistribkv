package db

import (
	bolt "go.etcd.io/bbolt"
	"log"
)

var (
	bucket = []byte("storage")
)

//Database is bolt. A fork of etcd
type Database struct {
	db *bolt.DB
}

func Close(database Database) {
	err := database.db.Close()
	if err != nil {
		log.Fatal("Database close failed. Hence exiting app")
	}
}

func NewDatabase(path string) (*Database, error) {
	bdb, err := bolt.Open(path, 0666, nil)
	if err != nil {
		return nil, err
	}
	database := &Database{db: bdb}
	err = database.createTransaction()
	if err != nil {
		Close(*database)
		return nil, err
	}
	return database, nil
}

func (d *Database) createTransaction() error {
	return d.db.Update(func(t *bolt.Tx) error {
		_, err := t.CreateBucketIfNotExists(bucket)
		return err
	})
}

func (d *Database) Set(key string, value []byte) error {
	return d.db.Update(func(t *bolt.Tx) error {
		b := t.Bucket(bucket)
		return b.Put([]byte(key), value)
	})
}

func (d *Database) Get(key string) ([]byte, error) {
	var value []byte
	err := d.db.View(func(t *bolt.Tx) error {
		b := t.Bucket(bucket)
		value = b.Get([]byte(key))
		return nil
	})

	return value, err
}
