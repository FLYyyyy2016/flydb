package my_db_code

import (
	log "github.com/sirupsen/logrus"
	"os"
	"syscall"
)

const KB = 1024
const MB = 1024 * KB
const GB = 1024 * MB

type DB struct {
	path     string
	file     *os.File
	dataSz   int
	dataRef  []byte
	pageList []page
}

func Open(path string) (db *DB, err error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	err = syscall.Flock(int(f.Fd()), syscall.LOCK_NB|syscall.LOCK_EX)
	db = &DB{path: path, file: f}
	if err != nil {
		return nil, err
	}

	info, err := db.file.Stat()
	if err != nil {
		log.Error(err)
	}
	if info.Size() != 0 {
		mmap(db, int(info.Size()))
	} else {
		log.Debug(info.Size())
		db.init()
		mmap(db, db.dataSz)
	}
	db.loadPages()
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

func (db *DB) init() error {
	buf := make([]byte, PageSize*4)

	metaPage := db.pageInBuffer(buf, 0)
	metaPage.id = 0
	metaPage.flag = metaPageType
	meta := metaPage.meta()
	meta.root = 2
	meta.freelist = 1

	freeListPage := db.pageInBuffer(buf, 1)
	freeListPage.id = 1
	freeListPage.flag = freelistPageType

	rootPage := db.pageInBuffer(buf, 2)
	rootPage.id = 2
	rootPage.flag = branchPageType

	valuePage := db.pageInBuffer(buf, 3)
	valuePage.id = 3
	valuePage.flag = leafPageType
	valuePage.node()

	db.dataRef = buf
	db.dataSz = len(buf)
	n, err := db.file.Write(buf)
	if err != nil {
		log.Error(err)
	} else {
		log.Debugf("init with write %d bytes", n)
	}
	err = syscall.Fdatasync(int(db.file.Fd()))
	if err != nil {
		log.Error(err)
	}
	return nil
}

func mmap(db *DB, sz int) error {
	b, err := syscall.Mmap(int(db.file.Fd()), 0, sz, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return err
	}
	err = syscall.Madvise(b, syscall.MADV_RANDOM)
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

func (db *DB) GetDataRef() []byte {
	return db.dataRef
}

func (db *DB) loadPages() {
	pageNum := len(db.dataRef) / PageSize
	db.pageList = make([]page, pageNum)
	log.Debugf("db has %d pages", pageNum)
	for i := 0; i < pageNum; i++ {
		db.pageList[i] = page{
			id:   pgid(i),
			flag: 1,
		}
	}
}

func (db *DB) Set(key int, value int) error {

	return nil
}
