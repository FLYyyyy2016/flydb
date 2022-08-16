package my_db_code

import (
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"testing"
)

func TestTxSetGet(t *testing.T) {
	db, err := Open(testFile)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = db.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()
	for i := 0; i < count; i++ {
		db.Update(func(tx *trans) {
			tx.Add(i, i)
		})
		db.View(func(tx *trans) {
			r := tx.Get(i)
			if !assert.Equal(t, i, r) {
				log.Fatal()
			}
		})
	}
}

func TestTxSetGetMany(t *testing.T) {
	db, err := Open(testFile)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = db.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()
	m := make(map[int]int)
	r := rand.NewSource(int64(source))
	for i := 0; i < count; i++ {
		num := int(r.Int63())
		m[num] = num
		db.Update(func(tx *trans) {
			tx.Add(num, num)
		})
	}
	for k, v := range m {
		db.View(func(tx *trans) {
			assert.Equal(t, v, tx.Get(k))
		})
	}

}
