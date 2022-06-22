package my_db_code

import (
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

var testFile = "test.db"

func TestDB_Set(t *testing.T) {
	db, err := Open(testFile)
	if err != nil {
		log.Fatal(err)
	}
	log.Println(db.Get(1))
	err = db.Set(1, time.Now().Nanosecond())
	if err != nil {
		t.Error(err)
	}
	log.Println(db.Get(1))
	err = db.Set(1, time.Now().Nanosecond())
	if err != nil {
		t.Error(err)
	}
	log.Println(db.Get(1))
	err = db.Set(2, time.Now().Nanosecond())
	if err != nil {
		t.Error(err)
	}
	log.Println(db.Get(2))
	err = db.Set(2, time.Now().Nanosecond())
	if err != nil {
		t.Error(err)
	}
	log.Println(db.Get(2))
	log.Println(db.Get(1))

	err = db.Close()
	if err != nil {
		log.Fatal(err)
	}
}

func TestDB_SetMany(t *testing.T) {
	var count = 128
	db, err := Open(testFile)
	if err != nil {
		log.Fatal(err)
	}
	m := make(map[int]int)
	for i := 0; i < count; i++ {
		now := time.Now()
		err := db.Set(i, now.Nanosecond())
		if err != nil {
			log.Error(err)
		}
		m[i] = now.Nanosecond()
	}
	for i := 0; i < count; i++ {
		result := db.Get(i)
		assert.Equal(t, m[i], result)
	}

	err = db.Close()
	if err != nil {
		log.Fatal(err)
	}
}
