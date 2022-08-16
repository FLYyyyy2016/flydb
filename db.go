package my_db_code

import (
	"os"
	"sync"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"
)

const KB = 1024
const MB = 1024 * KB
const GB = 1024 * MB

const initMetaPageId1 = 0
const initMetaPageId2 = 1
const initFreeListPageId = 2
const initRootPageId = 3

type DB struct {
	path     string
	file     *os.File
	dataSz   int
	dataRef  []byte
	pageList []page
	tempData []byte
	lock     sync.RWMutex
	tx       *trans
	txs      []*trans
}

type trans struct {
	change    map[pgid]pgid
	db        *DB
	writeable bool
}

func (db *DB) newTrans(writeable bool) *trans {
	return &trans{
		change:    make(map[pgid]pgid),
		db:        db,
		writeable: writeable,
	}
}

func (tx *trans) Add(key, value int) {
	metaPage := tx.db.getMeta()
	rootPgId := metaPage.root
	root := tx.db.getPage(rootPgId)
	rootNode := root.node()
	rootNode.set(key, value, tx.db, nil)

	return
}

func (tx *trans) Get(key int) int {
	m := tx.db.getMeta()
	rootPgId := m.root
	root := tx.db.getPage(rootPgId)
	rootNode := root.node()
	returnItem := rootNode.get(key, tx.db)
	if returnItem.notNull() {
		return returnItem.value
	}
	return -1
}

func (tx *trans) Delete(key int) {
	m := tx.db.getMeta()
	rootPgId := m.root
	root := tx.db.getPage(rootPgId)
	rootNode := root.node()
	rootNode.delete(key, tx.db, nil)
}

func (tx *trans) clearPage() {
	for k, _ := range tx.change {
		tx.db.getPage(k).flag = notUsedType
	}
}

func Open(path string) (db *DB, err error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	err = syscall.Flock(int(f.Fd()), syscall.LOCK_NB|syscall.LOCK_EX)
	db = &DB{path: path, file: f, lock: sync.RWMutex{}}
	if err != nil {
		return nil, err
	}
	db.tx = db.newTrans(true)
	for i := 0; i < 8; i++ {
		db.txs = append(db.txs, db.newTrans(false))
	}

	info, err := db.file.Stat()
	if err != nil {
		log.Error(err)
	}
	if info.Size() != 0 {
		err = mmap(db, int(info.Size()))
		if err != nil {
			log.Error(err)
		}
	} else {
		log.Debug(info.Size())
		err = db.init()
		if err != nil {
			log.Error(err)
		}
		err = mmap(db, db.dataSz)
		if err != nil {
			log.Error(err)
		}
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
	buf := make([]byte, PageSize*8)

	metaPage := db.pageInBuffer(buf, initMetaPageId1)
	metaPage.id = initMetaPageId1
	metaPage.flag = metaPageType
	metaNode := metaPage.meta()
	metaNode.root = initRootPageId
	metaNode.freelist = initFreeListPageId
	metaNode.txID = 2

	metaPage2 := db.pageInBuffer(buf, initMetaPageId2)
	metaPage2.id = initMetaPageId1
	metaPage2.flag = metaPageType
	metaNode2 := metaPage2.meta()
	metaNode2.root = initRootPageId
	metaNode2.freelist = initFreeListPageId
	metaNode.txID = 1

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

func (db *DB) Update(fn func(tx *trans)) {
	db.lock.Lock()
	defer db.lock.Unlock()
	db.tx = db.newTrans(true)
	fn(db.tx)
	db.tx.clearPage()
	meta1 := db.getMeta()
	meta2 := db.getTempMeta()
	meta2.txID = meta1.txID + 1
}

func (db *DB) View(fn func(tx *trans)) {
	db.lock.RLock()
	defer db.lock.RUnlock()
	fn(db.tx)
}

func mmap(db *DB, sz int) error {
	info, err := db.file.Stat()
	if err != nil {
		log.Error(err)
	}
	if info.Size() < int64(sz) {
		err = syscall.Ftruncate(int(db.file.Fd()), int64(sz))
		if err != nil {
			log.Error(err)
		}
	}
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

func (db *DB) getTempPage(id pgid) *page {
	m := db.tx.change
	if v, ok := m[id]; ok {
		return db.pageInBuffer(db.dataRef, v)
	}
	return db.pageInBuffer(db.dataRef, id)
}

func (db *DB) getMeta() *meta {
	meta1 := db.getPage(initMetaPageId1).meta()
	meta2 := db.getPage(initMetaPageId2).meta()
	if meta1.txID == meta2.txID {
		log.Error()
	}
	if meta1.txID > meta2.txID {
		return meta1
	}
	return meta2
}

func (db *DB) getTempMeta() *meta {
	meta1 := db.getPage(initMetaPageId1).meta()
	meta2 := db.getPage(initMetaPageId2).meta()
	if meta1.txID > meta2.txID {
		return meta2
	}
	return meta1
}

func (db *DB) loadPages() {
	pageNum := len(db.dataRef) / PageSize
	db.pageList = make([]page, pageNum)
	log.Debugf("db has %d pages", pageNum)
	for i := 0; i < pageNum; i++ {
		db.pageList[i] = page{
			id:   pgid(i),
			flag: db.getPage(pgid(i)).flag,
		}
	}
}

func (db *DB) Set(key int, value int) error {
	db.Update(func(tx *trans) {
		tx.Add(key, value)
	})

	return nil
}

func (db *DB) Get(key int) int {
	res := -1
	db.View(func(tx *trans) {
		res = tx.Get(key)
	})
	return res
}

func (db *DB) getNewPage() pgid {
	db.loadPages()
	for _, pg := range db.pageList {
		if pg.flag == notUsedType {
			thisNotUsedPage := db.getPage(pg.id)
			thisNotUsedPage.flag = branchPageType
			thisNotUsedPage.id = pg.id
			log.Debugf("return a new page %v", pg.id)
			return pg.id
		}
	}
	log.Debugf("expend file")
	db.expend()
	return db.getNewPage()
}

func (db *DB) Dump() {
	db.loadPages()
	for _, pageInfo := range db.pageList {
		switch pageInfo.flag {
		case metaPageType:
			p := db.getPage(pageInfo.id)
			log.Printf("meta page: txid is %v, root is %v, freelist is %v", p.meta().txID, p.meta().root, p.meta().freelist)
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
		case notUsedType:
			log.Printf("not used page %v", pageInfo.id)
		}
	}
}

func (db *DB) Delete(key int) {

	db.Update(func(tx *trans) {
		tx.Delete(key)
	})

	//m := db.getMeta()
	//rootPgId := m.root
	//root := db.getPage(rootPgId)
	//rootNode := root.node()
	//rootNode.delete(key, db, nil)
}

func (db *DB) removePage(id pgid) {
	delPage := db.getPage(id)
	delPage.flag = notUsedType
	db.loadPages()
}

func (db *DB) expend() {
	now := time.Now()
	oldSize := db.dataSz
	defer log.Debugf("cost %v, from %v to %v", time.Since(now), oldSize, oldSize*2)
	err := syscall.Fdatasync(int(db.file.Fd()))
	if err != nil {
		log.Error(err)
	}
	err = munmap(db)
	if err != nil {
		log.Error(err)
	}
	err = syscall.Flock(int(db.file.Fd()), syscall.LOCK_UN)
	if err != nil {
		log.Error(err)
	}
	err = mmap(db, oldSize*2)
	if err != nil {
		log.Error(err)
	}
	err = syscall.Flock(int(db.file.Fd()), syscall.LOCK_NB|syscall.LOCK_EX)
	if err != nil {
		log.Error(err)
	}
	err = syscall.Fdatasync(int(db.file.Fd()))
	if err != nil {
		log.Error(err)
	}
}

func (db *DB) copyTo(id pgid, temp pgid) {
	copy(db.dataRef[temp*PageSize:temp*PageSize+PageSize], db.dataRef[id*PageSize:id*PageSize+PageSize])
	db.getPage(temp).id = temp
	db.getPage(temp).node().pgId = temp
	db.tx.change[id] = temp
}
