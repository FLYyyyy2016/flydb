package my_db_code

import (
	"os"
	"syscall"
)

const KB = 1024
const MB = 1024 * KB
const GB = 1024 * MB

type DB struct {
	path    string
	file    *os.File
	dataSz  int
	dataRef []byte
}

func Open(path string) (db *DB, err error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	err = syscall.Flock(int(f.Fd()), syscall.LOCK_NB|syscall.LOCK_EX)
	db = &DB{path: path, file: f}
	if err != nil {
		return nil, err
	}
	// 开启1M
	err = mmap(db, 1000*MB)
	if err != nil {
		db.file.Close()
		return nil, err
	}
	return db, nil
}

func (db *DB) Close() error {
	f := db.file
	err := syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	if err != nil {
		return err
	}
	err = f.Close()
	if err != nil {
		return err
	}
	err = munmap(db)
	if err != nil {
		return err
	}
	return nil
}

func mmap(db *DB, sz int) error {
	b, err := syscall.Mmap(int(db.file.Fd()), 0, sz, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return err
	}
	err = syscall.Madvise(b, syscall.MADV_NORMAL)
	if err != nil {
		return err
	}
	db.dataRef = b
	db.dataSz = sz
	return nil
}

func munmap(db *DB) error {
	if db.dataRef == nil {
		return nil
	}
	err := syscall.Munmap(db.dataRef)
	db.dataSz = 0
	db.dataRef = nil
	return err
}

func (db *DB) GetData() []byte {
	return db.dataRef
}
