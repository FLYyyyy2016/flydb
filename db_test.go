package my_db_code

import (
	"github.com/onrik/logrus/filename"
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

func TestMain(m *testing.M) {
	log.AddHook(filename.NewHook())
	log.SetFormatter(&log.TextFormatter{DisableTimestamp: true})
	//log.SetLevel(log.DebugLevel)
	defer func() {
		log.Println("remove db file")
		err := os.Remove(testFile)
		if err != nil {
			log.Error(err)
		}
	}()
	m.Run()
}

func TestOpenClose(t *testing.T) {
	db, err := Open(testFile)
	if err != nil {
		t.Fatal(err)
	}
	err = db.Close()
	if err != nil {
		t.Fatal(err)
	}
}

// 使用了文件锁，所以重复用一个文件打开会失败
func TestMultiOpen(t *testing.T) {
	db, err := Open(testFile)
	if err != nil {
		log.Fatal(err)
	}
	_, err2 := Open(testFile)
	assert.Error(t, err2)
	err = db.Close()
	if err != nil {
		log.Fatal(err)
	}
}

// 基本使用
func TestDB_Set(t *testing.T) {
	db, err := Open(testFile)
	if err != nil {
		t.Fatal(err)
	}
	log.Println(db.Get(1))
	err = db.Set(1, time.Now().Nanosecond())
	if err != nil {
		t.Error(err)
	}
	t.Log(db.Get(1))
	err = db.Set(1, time.Now().Nanosecond())
	if err != nil {
		t.Error(err)
	}
	t.Log(db.Get(1))
	err = db.Set(2, time.Now().Nanosecond())
	if err != nil {
		t.Error(err)
	}
	t.Log(db.Get(2))
	err = db.Set(2, time.Now().Nanosecond())
	if err != nil {
		t.Error(err)
	}
	t.Log(db.Get(2))
	t.Log(db.Get(1))

	err = db.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestDB_SetTimeMany(t *testing.T) {
	var count = 64
	db, err := Open(testFile)
	if err != nil {
		t.Fatal(err)
	}
	m := make(map[int]int)
	for i := 0; i < count; i++ {
		now := time.Now()
		err := db.Set(i, now.Nanosecond())
		if err != nil {
			t.Error(err)
		}
		m[i] = now.Nanosecond()
	}
	for i := 0; i < count; i++ {
		result := db.Get(i)
		assert.Equal(t, m[i], result)
	}

	err = db.Close()
	if err != nil {
		t.Fatal(err)
	}
}

// 较为复杂的随机增删改查
func TestSetAndGetMany(t *testing.T) {
	//t.SetLevel(t.DebugLevel)
	dataList = make(map[int]int)
	t.Run("set many", setMany)
	t.Run("get many", getMany)
	t.Run("delete and get", deleteMany)
}

func setMany(t *testing.T) {
	db, err := Open(testFile)
	if err != nil {
		t.Fatal(err)
	}
	rd := rand.NewSource(int64(source))
	for i := 0; i < count; i++ {
		i := int(rd.Int63() % math.MaxInt16)
		err = db.Set(i, i)
		if err != nil {
			t.Error(err)
		}
		dataList[i] = i
		result := db.Get(i)
		assert.Equal(t, i, result)
	}
	for k, v := range dataList {
		result := db.Get(k)
		assert.Equal(t, v, result)
	}

	err = db.Close()
	if err != nil {
		t.Fatal(err)
	}
}
func getMany(t *testing.T) {
	db, err := Open(testFile)
	if err != nil {
		t.Fatal(err)
	}
	for k, _ := range dataList {
		result := db.Get(dataList[k])
		assert.Equal(t, dataList[k], result)
	}

	err = db.Close()
	if err != nil {
		t.Fatal(err)
	}
}
func deleteMany(t *testing.T) {
	db, err := Open(testFile)
	if err != nil {
		t.Fatal(err)
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
		t.Fatal(err)
	}
}

func BenchmarkSet(b *testing.B) {
	db, err := Open(testFile)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		err = db.Close()
		if err != nil {
			b.Fatal(err)
		}
	}()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = db.Set(i, i)
	}
}

func BenchmarkGet(b *testing.B) {
	db, err := Open(testFile)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		err = db.Close()
		if err != nil {
			b.Fatal(err)
		}
	}()
	maxKey := db.getMax()
	if maxKey == 0 {
		maxKey = math.MaxInt
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = db.Get(i % maxKey)
	}
}
