package my_db_code

import (
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"math"
	"math/rand"
	"os"
	"testing"
	"time"
)

var testFile = "test.db"
var count = 5000
var source = 10000
var dataList map[int]int

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
	//log.SetLevel(log.DebugLevel)
	dataList = make(map[int]int)
	t.Run("set many", setMany)
	t.Run("get many", getMany)
	t.Run("delete and get", deleteMany)
	err := os.Remove(testFile)
	if err != nil {
		t.Error(err)
	}
}

func setMany(t *testing.T) {
	db, err := Open(testFile)
	if err != nil {
		log.Fatal(err)
	}
	rd := rand.NewSource(int64(source))
	for i := 0; i < count; i++ {
		i := int(rd.Int63() % math.MaxInt16)
		err = db.Set(i, i)
		if err != nil {
			log.Error(err)
		}
		dataList[i] = i
		result := db.Get(i)
		if i != result {
			log.Println("set result error")
			db.Dump()
		}
		assert.Equal(t, i, result)
	}
	for k, v := range dataList {
		result := db.Get(k)
		assert.Equal(t, v, result)
	}

	err = db.Close()
	if err != nil {
		log.Fatal(err)
	}
}
func getMany(t *testing.T) {
	db, err := Open(testFile)
	if err != nil {
		log.Fatal(err)
	}
	for k, _ := range dataList {
		result := db.Get(dataList[k])
		assert.Equal(t, dataList[k], result)
	}

	err = db.Close()
	if err != nil {
		log.Fatal(err)
	}
}
func deleteMany(t *testing.T) {
	db, err := Open(testFile)
	if err != nil {
		log.Fatal(err)
	}
	m := make([]int, 0)
	for k, _ := range dataList {
		m = append(m, k)
	}
	r := rand.NewSource(100)
	for i := 0; i < len(m); i++ {
		index := int(r.Int63() % int64(len(m)))
		db.Delete(m[index])
		delete(dataList, m[index])
	}
	for k, v := range dataList {
		value := db.Get(k)
		assert.Equal(t, v, value)
	}
	err = db.Close()
	if err != nil {
		log.Fatal(err)
	}
}
