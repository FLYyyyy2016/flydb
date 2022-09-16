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
	path        string
	file        *os.File
	dataSz      int
	dataRef     []byte
	allPageList allPageList
	tempData    []byte
	lock        sync.RWMutex
	metaLock    sync.RWMutex
	tx          *trans
	txs         []*trans
}

type allPageList struct {
	size int
	root pgid
	db   *DB
}

func (list *allPageList) clearPage(id pgid) {
	list.getPageInfo(id).flag = notUsedType
}

func (list *allPageList) getPage(id pgid) *page {
	return &page{
		id:   id,
		flag: list.getPageInfo(id).flag,
	}
}

func (list *allPageList) getPageList() *pageList {
	root := list.root
	pg := list.db.getPage(root)
	pl := pg.pageList()
	return pl
}

func (list *allPageList) usePage(id pgid) {
	list.getPageInfo(id).flag = branchPageType
}

func (list *allPageList) getPageInfo(id pgid) *pageInfo {
	if int(id) > list.size {
		return nil
	}
	pl := list.getPageList()
	for id > pl.maxID {
		pl = list.db.getPage(pl.next).pageList()
	}
	return &pl.pageInfos()[id-pl.minID]
}

func (list *allPageList) expend(count int) {
	pl := list.getPageList()
	for pl.next != 0 {
		pl = list.db.getPage(pl.next).pageList()
	}
	if int(pl.maxID-pl.minID)+count > len(pl.pageInfos()) {
		diff := len(pl.pageInfos()) - int(pl.size+1)
		list.size += diff
		pl.maxID += pgid(diff)
		pl.size += diff
		newPage := list.db.getNewPage()
		np := list.db.getPage(newPage)
		np.flag = freelistPageType
		npl := np.pageList()
		npl.minID = pl.maxID + 1
		npl.maxID = npl.minID
		pl.next = newPage
		list.expend(count - diff)
	} else {
		list.size += count
		if pl.size == 0 {
			pl.maxID += pgid(count - 1)
		} else {
			pl.maxID += pgid(count)
		}
		pl.size += count
	}
}

type trans struct {
	change    map[pgid]pgid
	db        *DB
	writeable bool
	meta      *meta
}

func (db *DB) newTrans(writeable bool) *trans {
	return &trans{
		change:    make(map[pgid]pgid),
		db:        db,
		writeable: writeable,
		meta:      db.getMeta(),
	}
}

func (db *DB) loadPageList() {
	pageNum := len(db.dataRef) / PageSize
	db.allPageList = allPageList{
		size: pageNum,
		root: initFreeListPageId,
		db:   db,
	}
}

func (db *DB) createPageList() {
	pageNum := len(db.dataRef) / PageSize
	db.allPageList = allPageList{
		size: pageNum,
		root: initFreeListPageId,
		db:   db,
	}
	pl := db.allPageList.getPageList()
	if pgid(pageNum) > pl.maxID {
		pl.minID = 0
		pl.maxID = pgid(pageNum - 1)
		pl.size = pageNum
		pl.next = 0
		vs := pl.pageInfos()
		for i := 0; i < pl.size; i++ {
			pg := db.getPage(pgid(i))
			vs[i].flag = pg.flag
		}
	} else {
		log.Fatal("bad load")
	}
}

func (db *DB) Update(fn func(tx *trans)) {
	db.lock.Lock()
	defer db.lock.Unlock()
	db.tx = db.newTrans(true)
	fn(db.tx)

	db.metaLock.Lock()
	defer db.metaLock.Unlock()
	db.tx.clearPage()
	meta1 := db.getMeta()
	meta2 := db.getTempMeta()
	meta2.txID = meta1.txID + 1
}

func (db *DB) View(fn func(tx *trans)) {
	db.metaLock.RLock()
	defer db.metaLock.RUnlock()
	tx := db.newTrans(false)
	fn(tx)
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
	m := tx.meta
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
		tx.db.removePage(k)
	}
}

func Open(path string) (db *DB, err error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	err = syscall.Flock(int(f.Fd()), syscall.LOCK_NB|syscall.LOCK_EX)
	db = &DB{path: path, file: f, lock: sync.RWMutex{}, metaLock: sync.RWMutex{}}
	if err != nil {
		return nil, err
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
	db.createPageList()
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
func (db *DB) getFreePage(id pgid) *page {
	return db.allPageList.getPage(id)
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
	log.Debugf("db has %d pages", pageNum)
	db.loadPageList()
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
	for i := 0; i < db.allPageList.size; i++ {
		pg := db.allPageList.getPage(pgid(i))
		if pg.flag == notUsedType {
			thisNotUsedPage := db.getPage(pg.id)
			thisNotUsedPage.flag = branchPageType
			db.allPageList.usePage(pgid(i))
			thisNotUsedPage.id = pg.id
			log.Debugf("return a new page %v", pg.id)
			return pg.id
		}
	}
	if db.allPageList.size < len(db.dataRef)/PageSize {
		return pgid(db.allPageList.size + 1)
	}
	log.Debugf("expend file")
	db.expend()
	return db.getNewPage()
}

func (db *DB) Dump() {
	db.loadPages()
	for i := 0; i < db.allPageList.size; i++ {
		pageInfo := db.allPageList.getPage(pgid(i))
		switch pageInfo.flag {
		case metaPageType:
			p := db.getPage(pageInfo.id)
			log.Printf("page: %v,meta page: txid is %v, root is %v, freelist is %v", pageInfo.id, p.meta().txID, p.meta().root, p.meta().freelist)
		case branchPageType:
			p := db.getPage(pageInfo.id)
			n := p.node()
			if n.isBranch {
				log.Printf("page: %v,----------branch pgid is %v,data size is %v,maxkey is %v", pageInfo.id, n.pgId, n.size, n.maxKey)
				tn := n.treeNode()
				log.Printf("%v", tn.values[:n.size])
			} else {
				log.Printf("page: %v,=======treeNode pgid is %v,data size is %v,maxkey is %v", pageInfo.id, n.pgId, n.size, n.maxKey)
				tn := n.treeNode()
				log.Printf("%v", tn.values[:n.size])
			}
		case notUsedType:
			log.Printf("page: %v,not used page %v", pageInfo.id, pageInfo.id)
		case freelistPageType:
			p := db.getPage(pageInfo.id)
			pgList := p.pageList()
			log.Printf("page: %v,It is a list; size:%v,minID:%v,maxID:%v,next pgid:%v", pageInfo.id, pgList.size, pgList.minID, pgList.maxID, pgList.next)
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
	delNode := delPage.node()
	delNode.size = 0
	delNode.maxKey = 0
	delNode.isBranch = false
	delNode.pgId = 0
	tn := delNode.treeNode()
	tn.maxKey = 0
	delPage.flag = notUsedType
	db.allPageList.clearPage(id)
}

func (db *DB) expend() {
	db.metaLock.Lock()
	defer db.metaLock.Unlock()
	now := time.Now()
	oldSize := db.dataSz
	defer log.Debugf("cost %v, from %v to %v, add %v pages", time.Since(now), oldSize, oldSize*2, oldSize/PageSize)
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
	db.allPageList.expend(oldSize / PageSize)
}

func (db *DB) copyTo(id pgid, temp pgid) {
	copy(db.dataRef[temp*PageSize:temp*PageSize+PageSize], db.dataRef[id*PageSize:id*PageSize+PageSize])
	db.getPage(temp).id = temp
	db.getPage(temp).node().pgId = temp
	db.tx.change[id] = temp
}

func (db *DB) getMax() int {
	rootPageID := db.getMeta().root
	root := db.getPage(rootPageID)
	return root.node().maxKey
}
