package main

import (
	"github.com/FLYyyyy2016/my-db-code"
	log "github.com/sirupsen/logrus"
)

var testFile = "test.db"

func main() {
	step1()
}

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
