package bolt

import (
	"os"
	"spWebFront/FrontKeeper/infrastructure/core"
	"spWebFront/FrontKeeper/infrastructure/log"

	"github.com/boltdb/bolt"
)

const (
	DatabaseVersion = "1"
)

type Invalidator interface {
	// Database is invalid
	Invalidate()
}

type DB interface {
	// Recovery new database
	Recovery(action func() error, guid string) error
	RecoveryInit(guid string) error
	RecoveryDone() error
	// Close database
	Close() error
	// Start new transaction
	Begin(writable bool) (Tx, error)
	// Update database in transaction
	Update(fn func(Tx) error) error
	// Read database from transaction
	View(fn func(Tx) error) error
	//
	Batch(fn func(Tx) error) error
	// Get statistics
	Stats() Stats
	// Get addition information
	Info() *Info
	// Get path to the database
	Path() string
	// Get string representation of database
	String() string
}

type Cursor interface {
	// Get related bucket
	Bucket() Bucket
	// Get first item of the sequence
	First() (key []byte, value []byte)
	// Get last item of the sequence
	Last() (key []byte, value []byte)
	// Get next item of the sequence
	Next() (key []byte, value []byte)
	// Get prev item of the sequence
	Prev() (key []byte, value []byte)
	// Seek cursor
	Seek(seek []byte) (key []byte, value []byte)
	// Delete current item from cursor
	Delete() error
}

type Container interface {
	// Container is writable
	Writable() bool
	// Get cursor for container
	Cursor() Cursor
	// Get child bucket for container
	Bucket(name []byte) Bucket
	// Create new child bucket in related bucket
	CreateBucket(name []byte) (Bucket, error)
	// Create new child bucket in related container if it is not exists
	CreateBucketIfNotExists(key []byte) (Bucket, error)
	// Delete child bucket from containe
	DeleteBucket(key []byte) error
}

type Tx interface {
	Container
	// Get database
	DB() DB
	// Get size of changed data
	Size() int64
	// Get statistics
	Stats() TxStats
	// Commit transaction
	Commit() error
	// Rollback transaction
	Rollback() error
}

type Bucket interface {
	Container
	// Get related transaction
	Tx() Tx
	// Get child value
	Get(key []byte) []byte
	// Put child pair (ke-value)
	Put(key []byte, value []byte) error
	// Delete child key
	Delete(key []byte) error
	// Get value of sequence counter
	Sequence() uint64
	// Set value of sequence counter
	SetSequence(v uint64) error
	// Get new value of the sequence counter
	NextSequence() (uint64, error)
	// Iterate by key-value pairs
	ForEach(fn func(k, v []byte) error) error
	// Get statistics for the bucket
	Stats() BucketStats
}

type Stats = bolt.Stats
type TxStats = bolt.TxStats
type Info = bolt.Info
type BucketStats = bolt.BucketStats

const (
	SystemBucketName = "system"   // Название системной папки
	anchorKey        = "anchor"   // Привязка базы к рабочей станции
	recoveryKey      = "recovery" // Флаг активности режима восстановления
)

var (
	SystemBucketNameBinary = []byte(SystemBucketName)
)

type db struct {
	*bolt.DB
	Invalidator
}

func (db *db) wrapError(err error, label string) error {
	if err == nil || err == core.ErrAbort {
		return nil
	}

	return db.wrapErrorEx(err, label, true)
}

func (db *db) wrapErrorEx(err error, label string, fatal bool) error {
	if err == nil || err == core.ErrAbort {
		return nil
	}

	if db.Invalidator != nil {
		code, _, _ := core.ExtractError(err)
		if code == core.ErrorDatabase.Code {
			db.Invalidator.Invalidate()
			return err
		}
		//core.LogError(err)
	}

	return wrapErrorEx(err, label, fatal)
}

func (db *db) wrapErrorWithControl(err error, label string, wrap bool) error {
	if err == nil || !wrap || err == core.ErrAbort {
		return err
	}
	code, _, _ := core.ExtractError(err)
	if code == core.ErrorDatabase.Code {
		return err
	}
	return db.wrapError(err, label)
}

func (db *db) Recovery(
	action func() error,
	guid string,
) error {
	log.Debugln("Recovery staring")
	defer log.Debugln("Recovery finished")

	err := db.RecoveryInit(guid)
	if err != nil {
		return err
	}

	err = action()
	if err != nil {
		e := core.Cause(err)
		if e == core.ErrNotAuthorized {
			return nil
		}
		return db.wrapErrorEx(err, "bolt.db.recovery.run", false)
	}

	return db.RecoveryDone()
}

func (db *db) RecoveryInit(
	guid string,
) error {
	err := db.Update(
		func(tx Tx) error {
			b, err := tx.CreateBucketIfNotExists(SystemBucketNameBinary)
			if err != nil {
				return err
			}

			err = b.Put([]byte(recoveryKey), []byte("true"))
			if err != nil {
				return err
			}

			value := getDatabaseId(guid)
			err = b.Put([]byte(anchorKey), []byte(value))
			if err != nil {
				return err
			}

			return nil
		},
	)
	if err != nil {
		return db.wrapError(err, "bolt.db.recovery.init")
	}
	return nil
}

func (db *db) RecoveryDone() error {
	err := db.Update(func(tx Tx) error {
		b := tx.Bucket(SystemBucketNameBinary)
		if b == nil {
			return nil
		}
		return b.Delete([]byte(recoveryKey))
	})
	if err != nil {
		return db.wrapError(err, "bolt.db.recovery.done")
	}

	return nil
}

func (db *db) Begin(writable bool) (Tx, error) {
	t, err := db.DB.Begin(writable)
	if err != nil {
		return nil, db.wrapError(err, "bolt.db.begin")
	}
	return &tx{
		Tx:     t,
		db:     db,
		bucket: nil,
	}, nil
}

func (db *db) Update(fn func(Tx) error) error {
	wrap := true
	err := db.DB.Update(
		func(t *bolt.Tx) error {
			err := fn(
				&tx{
					Tx:     t,
					db:     db,
					bucket: nil,
				},
			)
			wrap = err != nil
			return err
		},
	)
	return db.wrapErrorWithControl(err, "bolt.db.Update", wrap)
}

func (db *db) View(fn func(Tx) error) error {
	wrap := true
	err := db.DB.View(
		func(t *bolt.Tx) error {
			err := fn(
				&tx{
					Tx:     t,
					db:     db,
					bucket: nil,
				},
			)
			wrap = err != nil
			return err
		},
	)
	return db.wrapErrorWithControl(err, "bolt.db.View", wrap)
}

func (db *db) Batch(fn func(Tx) error) error {
	wrap := true
	err := db.DB.Batch(
		func(t *bolt.Tx) error {
			err := fn(
				&tx{
					Tx:     t,
					db:     db,
					bucket: nil,
				},
			)
			wrap = err != nil
			return err
		},
	)
	return db.wrapErrorWithControl(err, "bolt.db.Batch", wrap)
}

type cursor struct {
	*bolt.Cursor
	bucket *bucket
}

func (cur *cursor) wrapError(err error, label string) error {
	if err == nil {
		return nil
	}

	return cur.bucket.wrapError(err, label)
}

func (cur *cursor) Bucket() Bucket {
	return cur.bucket
}

func (cur *cursor) Delete() error {
	return cur.wrapError(
		cur.Cursor.Delete(),
		"bolt.db.cursor.delete",
	)
}

type tx struct {
	*bolt.Tx
	db     *db
	bucket *bucket
}

func (tx *tx) wrapError(err error, label string) error {
	if err == nil {
		return nil
	}

	return tx.db.wrapError(err, label)
}

func (tx *tx) DB() DB {
	return tx.db
}

func (tx *tx) Cursor() Cursor {
	return &cursor{
		Cursor: tx.Tx.Cursor(),
		bucket: tx.bucket,
	}
}

func (tx *tx) Bucket(name []byte) Bucket {
	b := tx.Tx.Bucket(name)
	if b == nil {
		return nil
	}
	return &bucket{
		tx:     tx,
		bucket: b,
	}
}

func (tx *tx) CreateBucket(name []byte) (Bucket, error) {
	//log.Println("tx.CreateBucket", string(name))
	b, err := tx.Tx.CreateBucket(name)
	if err != nil {
		return nil, tx.wrapError(err, "bolt.tx.createBucket")
	}
	return &bucket{
		tx:     tx,
		bucket: b,
	}, nil
}

func (tx *tx) CreateBucketIfNotExists(key []byte) (Bucket, error) {
	//log.Println("tx.CreateBucketIfNotExists", string(key))
	b, err := tx.Tx.CreateBucketIfNotExists(key)
	if err != nil {
		return nil, tx.wrapError(err, "bolt.tx.createBucketIfNotExists")
	}
	return &bucket{
		tx:     tx,
		bucket: b,
	}, nil
}

func (tx *tx) DeleteBucket(key []byte) error {
	//log.Println("DeleteBucket", string(key))
	return tx.wrapError(
		tx.Tx.DeleteBucket(key),
		"bolt.tx.DeleteBucket",
	)
}

type bucket struct {
	bucket *bolt.Bucket
	tx     *tx
}

func (b *bucket) wrapError(err error, label string) error {
	if err == nil {
		return nil
	}

	return b.tx.wrapError(err, label)
}

func (b *bucket) wrapErrorWithControl(err error, label string, wrap bool) error {
	if err == nil || !wrap {
		return err
	}
	if err == core.ErrAbort {
		return err
	}
	code, _, _ := core.ExtractError(err)
	switch code {
	case core.ErrorDatabase.Code:
		return err
	}
	return b.wrapError(err, label)
}

func (b *bucket) Cursor() Cursor {
	c := b.bucket.Cursor()
	return &cursor{
		Cursor: c,
		bucket: b,
	}
}

func (b *bucket) Bucket(name []byte) Bucket {
	bb := b.bucket.Bucket(name)
	if bb == nil {
		return nil
	}
	return &bucket{
		tx:     b.tx,
		bucket: bb,
	}
}

func (b *bucket) CreateBucket(name []byte) (Bucket, error) {
	//log.Println("b.CreateBucket", string(name))
	bb, err := b.bucket.CreateBucket(name)
	if err != nil {
		return nil, b.wrapError(err, "bolt.bucket.createBucket")
	}
	return &bucket{
		tx:     b.tx,
		bucket: bb,
	}, nil
}

func (b *bucket) CreateBucketIfNotExists(key []byte) (Bucket, error) {
	//log.Println("b.CreateBucketIfNotExists", string(key))
	bb, err := b.bucket.CreateBucketIfNotExists(key)
	if err != nil {
		return nil, b.wrapError(err, "bolt.bucket.CreateBucketIfNotExists")
	}
	return &bucket{
		tx:     b.tx,
		bucket: bb,
	}, nil
}

func (b *bucket) DeleteBucket(key []byte) error {
	//log.Println("b.CreateBucket", string(key))
	return b.wrapError(
		b.bucket.DeleteBucket(key),
		"bolt.bucket.DeleteBucket",
	)
}

func (b *bucket) Tx() Tx {
	return b.tx
}

func (b *bucket) Get(key []byte) []byte {
	return b.bucket.Get(key)
}

func (b *bucket) Put(key []byte, value []byte) error {
	// return b.wrapError(
	// 	errors.New("Testing error"),
	// 	"bolt.bucket.put",
	// )
	return b.wrapError(
		b.bucket.Put(key, value),
		"bolt.bucket.Put",
	)
}

func (b *bucket) Delete(key []byte) error {
	return b.wrapError(
		b.bucket.Delete(key),
		"bolt.bucket.Delete",
	)
}

func (b *bucket) Sequence() uint64 {
	return b.bucket.Sequence()
}

func (b *bucket) SetSequence(v uint64) error {
	return b.wrapError(
		b.bucket.SetSequence(v),
		"bolt.bucket.SetSequence",
	)
}

func (b *bucket) NextSequence() (uint64, error) {
	val, err := b.bucket.NextSequence()
	if err != nil {
		return 0, b.wrapError(err, "bolt.bucket.NextSequence")
	}
	return val, nil
}

func (b *bucket) ForEach(fn func(k, v []byte) error) error {
	wrap := true
	err := b.bucket.ForEach(
		func(k, v []byte) error {
			err := fn(k, v)
			wrap = err != nil
			return err
		},
	)
	return b.wrapErrorWithControl(err, "bolt.bucket.ForEach", wrap)
}

func (b *bucket) Stats() BucketStats {
	return b.bucket.Stats()
}

func (b *bucket) Writable() bool {
	return b.bucket.Writable()
}

func Open(
	filename string,
	invalidator Invalidator,
) (db0 DB, err error) {
	defer core.Recover(&err)

	db1, err := bolt.Open(filename, 0660, nil)
	if err != nil {
		return nil, wrapError(err, "bolt.Open")
	}

	err = db1.Update(
		func(tx *bolt.Tx) error {
			_, err := tx.CreateBucketIfNotExists(SystemBucketNameBinary)
			if err != nil {
				return wrapError(err, "bolt.Open2")
			}

			return nil
		},
	)

	return &db{
		DB:          db1,
		Invalidator: invalidator,
	}, nil
}

func OpenEx(
	filename string,
	stationId string,
	invalidator Invalidator,
) (db DB, err error) {
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		return nil, nil
	}

	db, err = Open(filename, invalidator)
	if err != nil || db == nil {
		return nil, nil
	}

	err = db.View(func(tx Tx) error {
		b := tx.Bucket(SystemBucketNameBinary)
		if b == nil {
			db.Close()
			db = nil
			return nil
		}

		recovery := b.Get([]byte(recoveryKey))
		if recovery != nil {
			db.Close()
			db = nil
			return nil
		}

		anchor := b.Get([]byte(anchorKey))
		if anchor == nil {
			db.Close()
			db = nil
			return nil
		}

		dbId := getDatabaseId(stationId)
		if string(anchor) != dbId {
			db.Close()
			db = nil
			return nil
		}

		return nil
	})

	return
}

func getDatabaseId(guid string) string {
	return guid + "-v" + DatabaseVersion
}

func wrapError(err error, label string) error {
	return wrapErrorEx(err, label, true)
}

func wrapErrorEx(err error, label string, fatal bool) error {
	if err == nil || err == core.ErrAbort {
		return nil
	}

	if fatal {
		// log.Println("DATABASE ERROR:: ", label, err)
		// panic(core.WrapError(core.ErrorDatabase, label, err))

		return core.WrapError(core.ErrorDatabase, label, err)
	}

	return err
}

func wrapError2(err error, label string) error {
	if err == nil || err == core.ErrAbort {
		return nil
	}
	code, _, _ := core.ExtractError(err)
	if code == core.ErrorDatabase.Code {
		return err
	}
	return wrapError(err, label)
}

type MuteDB interface {
	DB
	IsAlive() bool
	Mute(db DB)
}

type muteDB struct {
	DB
}

func (db *muteDB) IsAlive() bool {
	return db.DB != nil
}

func (db *muteDB) Mute(d DB) {
	db.Close()
	db.DB = d
}

func (db *muteDB) Close() error {
	d := db.DB
	if d == nil {
		return nil
	}
	db.DB = nil
	return d.Close()
}

func NewMuteDB() MuteDB {
	return &muteDB{}
}
