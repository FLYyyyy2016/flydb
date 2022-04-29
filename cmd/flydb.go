package main

import (
	"math/rand"
	"os"
	"time"

	"github.com/FLYyyyy2016/my-db-code"
	log "github.com/sirupsen/logrus"
)

var testFile = "hello.db"

func main() {
	log.SetLevel(log.DebugLevel)
	step5()
}

//正常打开数据库
func step1() {
	db, err := my_db_code.Open(testFile)
	if err != nil {
		log.Fatal(err)
	}
	err = db.Close()
	if err != nil {
		log.Fatal(err)
	}
}

// 该函数会因为占用testFile的文件锁占用失败而报错
func step2() {
	db, err := my_db_code.Open(testFile)
	if err != nil {
		log.Fatal(err)
	}
	db2, err := my_db_code.Open(testFile)
	if err != nil {
		log.Fatal(err)
	}
	err = db.Close()
	if err != nil {
		log.Fatal(err)
	}
	err = db2.Close()
	if err != nil {
		log.Fatal(err)
	}
}

// 尝试查看随机读取数据的性能
func step3() {
	db, err := my_db_code.Open(testFile)
	if err != nil {
		log.Fatal(err)
	}
	data := db.GetDataRef()
	log.Println(len(data))
	randomRead(data)
	err = db.Close()
	if err != nil {
		log.Fatal(err)
	}
}

// 1G数据随机访问1亿次需要使用5.7s
func randomRead(data []byte) {
	count := 0
	t := time.Now()
	for i := 0; i < 100000000; i++ {
		j := rand.Intn(len(data) - 5)
		d := data[j : j+5]
		for x := 0; x < len(d); x++ {
			count += int(d[x])
		}
	}
	log.Println("cost", time.Now().Sub(t), "get count", count)
}

//查看以正常文件io读取而不是mmap方式读取数据有多慢
// 1G数据随机访问1kw次需要使用10s，和mmap方式相比慢了20倍
func step4() {
	f, err := os.Open(testFile)
	if err != nil {
		log.Fatal(err)
	}
	info, err := f.Stat()
	if err != nil {
		log.Fatal(err)
	}
	count := 0
	t := time.Now()
	for i := 0; i < 10000000; i++ {
		j := rand.Intn(int(info.Size()) - 5)
		b := make([]byte, 5)
		d, err := f.ReadAt(b, int64(j))
		if err != nil {
			log.Fatal(err)
		}
		for x := 0; x < d; x++ {
			count += int(b[x])
		}
	}
	log.Println("cost", time.Now().Sub(t), "get count", count)
}

func step5() {
	db, err := my_db_code.Open(testFile)
	if err != nil {
		log.Fatal(err)
	}

	err = db.Close()
	if err != nil {
		log.Fatal(err)
	}
}
