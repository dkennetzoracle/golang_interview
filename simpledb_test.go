package simpledb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDB_EmptyUnset(t *testing.T) {
	db := NewDB()
	// Error on empty unset.
	require.Error(t, db.Unset("a"))
}

func TestDB_Unset(t *testing.T) {
	// Simple get and unset test.
	db := NewDB()
	db.Set("a", 10)
	v := db.Get("a")
	require.Equal(t, 10, v)

	err := db.Unset("a")
	require.NoError(t, err)

	v = db.Get("a")
	require.Nil(t, v)
}

func TestDB_Rollback(t *testing.T) {
	db := NewDB()

	// This block represents a nested rollback.
	// Start tx 1, set a value.
	db.Begin()
	db.Set("a", 10)
	v := db.Get("a")
	require.Equal(t, v, 10)

	// Start tx 2, set a value.
	db.Begin()
	db.Set("a", 20)
	v = db.Get("a")
	require.Equal(t, v, 20)

	// Rollback from tx 2 to tx 1.
	err := db.Rollback()
	require.NoError(t, err)
	v = db.Get("a")
	require.Equal(t, v, 10)

	// Rollback tx 1 to starting state.
	err = db.Rollback()
	require.NoError(t, err)
	v = db.Get("a")
	require.Nil(t, v)
}

func TestDB_NestedCommit(t *testing.T) {
	db := NewDB()

	// Nest tx'es, last value should be retrieved.
	db.Begin()
	db.Set("a", 30)

	db.Begin()
	db.Set("a", 40)
	err := db.Commit()
	require.Nil(t, err)

	v := db.Get("a")
	require.Equal(t, v, 40)

	// No tx in progress, rollback and commit should error.
	require.Error(t, db.Rollback())
	require.Error(t, db.Commit())
}

func TestDB_TransactionInterleavedKeys(t *testing.T) {
	db := NewDB()

	// Immediate commit outside of a tx.
	db.Set("a", 10)
	db.Set("b", 10)
	va := db.Get("a")
	vb := db.Get("b")
	require.Equal(t, va, 10)
	require.Equal(t, vb, 10)

	// "a" should be updated, "b" should be propagated.
	db.Begin()
	db.Set("a", 20)
	va = db.Get("a")
	vb = db.Get("b")
	require.Equal(t, va, 20)
	require.Equal(t, vb, 10)

	// "b" should be updated, "a" should be propagated.
	db.Begin()
	db.Set("b", 30)
	va = db.Get("a")
	vb = db.Get("b")
	require.Equal(t, va, 20)
	require.Equal(t, vb, 30)
}

func TestDB_TransactionRollbackUnset(t *testing.T) {
	db := NewDB()

	// Immediate commit outside of a tx.
	db.Set("a", 10)
	v := db.Get("a")
	require.Equal(t, v, 10)

	// "a" should be accessible in tx.
	db.Begin()
	v2 := db.Get("a")
	require.Equal(t, v2, 10)

	// "a" should be updated in tx.
	db.Set("a", 20)
	v3 := db.Get("a")
	require.Equal(t, v3, 20)

	// "a" should be nil inside the tx after unset.
	db.Begin()
	err := db.Unset("a")
	require.NoError(t, err)

	v4 := db.Get("a")
	require.Nil(t, v4)

	// "a" should return to previous value after rollback.
	db.Rollback()
	v5 := db.Get("a")
	require.Equal(t, v5, 20)

	// "a" should equal the last set value after rollback + commit.
	db.Commit()
	v6 := db.Get("a")
	require.Equal(t, v6, 20)
}

func TestDB_TransactionCommitUnset(t *testing.T) {
	db := NewDB()

	// Immediate commit outside of a tx.
	db.Set("a", 10)
	v := db.Get("a")
	require.Equal(t, v, 10)

	// Unsetting in tx gets rolled back.
	db.Begin()
	v2 := db.Get("a")
	require.Equal(t, v2, 10)

	err := db.Unset("a")
	require.Nil(t, err)

	v2 = db.Get("a")
	require.Nil(t, v2)

	err = db.Rollback()
	require.Nil(t, err)

	v3 := db.Get("a")
	require.Equal(t, v3, 10)

	// Committing an empty transaction is not an error.
	db.Begin()
	err = db.Unset("a")
	require.Nil(t, err)

	v4 := db.Get("a")
	require.Nil(t, v4)

	err = db.Commit()
	require.Nil(t, err)

	v5 := db.Get("a")
	require.Nil(t, v5)

	// "a" should not be committed to db at this point, because it was unset and committed.
	db.Begin()
	v6 := db.Get("a")
	require.Nil(t, v6)

	db.Set("a", 20)
	v7 := db.Get("a")
	require.Equal(t, v7, 20)

	// "a" should be 20 after final commit.
	err = db.Commit()
	require.Nil(t, err)
	v8 := db.Get("a")
	require.Equal(t, v8, 20)
}
