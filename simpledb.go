// Package simpledb provides a basic in-memory key-value database with transaction support.
package simpledb

// DB represents a simple key-value database.
type DB struct{}

// NewDB creates and returns a new instance of DB.
func NewDB() *DB {
	return &DB{}
}

// Set stores the given value for the specified key in the database.
// If the key already exists, its value is overwritten.
func (db *DB) Set(key string, val interface{}) {}

// Get retrieves the value associated with the given key from the database.
// It returns nil if the key does not exist.
func (db *DB) Get(key string) interface{} {
	return nil
}

// Unset removes the specified key and its associated value from the database.
// It returns an error if the key does not exist or if the operation fails.
func (db *DB) Unset(key string) error {
	return nil
}

// Begin starts a new transaction.
func (db *DB) Begin() {}

// Commit applies all changes made in the current transaction to the database.
// It returns an error if the commit operation fails.
func (db *DB) Commit() error {
	return nil
}

// Rollback discards all changes made in the current transaction.
// It returns an error if the rollback operation fails.
func (db *DB) Rollback() error {
	return nil
}
