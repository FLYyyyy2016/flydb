package my_db_code

import (
	log "github.com/sirupsen/logrus"
	"os"
	"syscall"
)

const KB = 1024
const MB = 1024 * KB
const GB = 1024 * MB

const initMetaPageId = 0
const initFreeListPageId = 1
const initRootPageId = 2
const initLeafPageId = 3

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
	buf := make([]byte, PageSize*300)

	metaPage := db.pageInBuffer(buf, initMetaPageId)
	metaPage.id = initMetaPageId
	metaPage.flag = metaPageType
	meta := metaPage.meta()
	meta.root = initRootPageId
	meta.freelist = initFreeListPageId

	freeListPage := db.pageInBuffer(buf, initFreeListPageId)
	freeListPage.id = initFreeListPageId
	freeListPage.flag = freelistPageType

	rootPage := db.pageInBuffer(buf, initRootPageId)
	rootPage.id = initRootPageId
	rootPage.flag = branchPageType
	rootNode := rootPage.node()
	rootNode.pgId = rootPage.id

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
	b, err := syscall.Mmap(int(db.file.Fd()), 0, sz, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
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

func (db *DB) getPage(id pgid) *page {
	return db.pageInBuffer(db.dataRef, id)
}

func (db *DB) loadPages() {
	pageNum := len(db.dataRef) / PageSize
	db.pageList = make([]page, pageNum)
	log.Debugf("db has %d pages", pageNum)
	for i := 0; i < pageNum; i++ {
		db.pageList[i] = page{
			id:   pgid(i),
			flag: db.pageInBuffer(db.dataRef, pgid(i)).flag,
		}
	}
}

func (db *DB) Set(key int, value int) error {
	meta := db.pageInBuffer(db.dataRef, initMetaPageId).meta()
	rootPgId := meta.root
	root := db.pageInBuffer(db.dataRef, rootPgId)
	node := root.node()
	node.set(key, value, db, nil)

	//treeNode := node.treeNode()
	//item := treeNode.get(key)
	//if item.notNull() {
	//	item.value = value
	//	return nil
	//}
	//treeNode.add(key, value)
	return nil
}

func (db *DB) Get(key int) int {
	rootPgId := db.pageInBuffer(db.dataRef, 0).meta().root
	root := db.pageInBuffer(db.dataRef, rootPgId)
	node := root.node()
	item := node.get(key, db)
	//treeNode := node.treeNode()
	//item := treeNode.get(key)
	if item.notNull() {
		return item.value
	}
	return -1
}

func (db *DB) getNewPage() pgid {
	db.loadPages()
	for _, page := range db.pageList {
		if page.flag == notUsedType {
			thisNotUsedPage := db.pageInBuffer(db.dataRef, page.id)
			thisNotUsedPage.flag = branchPageType
			thisNotUsedPage.id = page.id
			return page.id
		}
	}
	// todo: 创建新页面
	return -1
}

func (db *DB) Dump() {
	db.loadPages()
	for _, pageInfo := range db.pageList {
		switch pageInfo.flag {
		case metaPageType:
			p := db.getPage(pageInfo.id)
			log.Printf("meta page: root is %v, freelist is %v", p.meta().root, p.meta().freelist)
		case branchPageType:
			p := db.getPage(pageInfo.id)
			n := p.node()
			if n.isBranch {
				log.Printf("----------branch pgid is %v,data size is %v,maxkey is %v", n.pgId, n.size, n.maxKey)
				tn := n.treeNode()
				log.Printf("%v", tn.values[:n.size])
			} else {
				log.Printf("=======treeNode pgid is %v,data size is %v,maxkey is %v", n.pgId, n.size, n.maxKey)
				tn := n.treeNode()
				log.Printf("%v", tn.values[:n.size])
			}
		}
	}
}

func (db *DB) Delete(key int) {
	rootPgId := db.pageInBuffer(db.dataRef, 0).meta().root
	root := db.pageInBuffer(db.dataRef, rootPgId)
	node := root.node()
	node.delete(key, db, nil)
}

func (db *DB) removePage(id pgid) {
	delPage := db.getPage(id)
	delPage.flag = notUsedType
	db.loadPages()
}
