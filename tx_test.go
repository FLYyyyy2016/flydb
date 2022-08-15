package my_db_code

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestTxSetGet(t *testing.T) {
	db, err := Open(testFile)
	if err != nil {
		t.Fatal(err)
	}
	db.Update(func(tx *trans) {
		tx.Add(1, 1)
	})
	db.View(func(tx *trans) {
		assert.Equal(t, tx.Get(1), 1)
	})
}
