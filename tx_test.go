package my_db_code

import (
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"sync"
	"testing"
	"time"
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

func TestSync(t *testing.T) {
	t.Run("ReadUnRepeatable", testDBReadUnRepeat)
}

func testDBReadUnRepeat(t *testing.T) {

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

	c1 := make(chan struct{})
	c2 := make(chan struct{})
	wg := sync.WaitGroup{}

	var k, v = 1, 1
	db.Update(func(tx *trans) {
		tx.Add(k, v)
	})
	wg.Add(2)
	go func() {
		db.Update(func(tx *trans) {
			tx.Add(k, v+1)
			tx.Add(k, v+2)
			c1 <- struct{}{}
			time.Sleep(100 * time.Millisecond)
		})
		c2 <- struct{}{}
		wg.Done()
	}()

	go func() {
		db.View(func(tx *trans) {
			assert.Equal(t, v, tx.Get(k))
			<-c1
			assert.Equal(t, v, tx.Get(k))
		})
		<-c2
		db.View(func(tx *trans) {
			assert.Equal(t, v+2, tx.Get(k))
		})
		wg.Done()
	}()
	wg.Wait()
}
