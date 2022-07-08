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

func TestDB_SetTimeMany(t *testing.T) {
	var count = 64
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

func TestSetAndGetMany(t *testing.T) {
	t.Run("set many", setMany)
	t.Run("get many", getMany)
}

func setMany(t *testing.T) {
	var count = 1024
	db, err := Open(testFile)
	if err != nil {
		log.Fatal(err)
	}
	m := make(map[int]int)
	for i := 0; i < count; i++ {
		if i == 252 {
			log.Println(testFile)
		}
		err := db.Set(i, i)
		if err != nil {
			log.Error(err)
		}
		m[i] = i
	}
	for i := 0; i < count; i++ {
		if i == 252 {
			log.Println(testFile)
		}
		result := db.Get(i)
		assert.Equal(t, m[i], result)
	}

	err = db.Close()
	if err != nil {
		log.Fatal(err)
	}
}
func getMany(t *testing.T) {
	var count = 1024
	db, err := Open(testFile)
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < count; i++ {
		result := db.Get(i)
		assert.Equal(t, i, result)
	}

	err = db.Close()
	if err != nil {
		log.Fatal(err)
	}
}
