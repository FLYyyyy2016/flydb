package my_db_code

import (
	log "github.com/sirupsen/logrus"
	"math"
	"unsafe"
)

const PageSize = 4 * 1024
const itemSize = 150
const listSize = 150

//const PageSize = 128
//const itemSize = 4

type pgid int

const (
	notUsedType      = 0
	metaPageType     = 1
	freelistPageType = 2
	branchPageType   = 3
	leafPageType     = 4
)

type page struct {
	id   pgid
	flag int
}

func (db *DB) pageInBuffer(b []byte, id pgid) *page {
	return (*page)(unsafe.Pointer(&b[id*PageSize]))
}

type meta struct {
	freelist pgid
	root     pgid
	txID     int
}

func (m *meta) clear() {

}

func (p *page) meta() *meta {
	return (*meta)(unsafe.Pointer(uintptr(unsafe.Pointer(p)) + unsafe.Sizeof(*p)))
}

type pageList struct {
	maxID pgid
	minID pgid
	next  pgid
	size  int
	list  freeList
}

type freeList []pageInfo

type pageInfo struct {
	flag int
}

func (p *page) pageList() *pageList {
	ptr := unsafeAdd(unsafe.Pointer(p), unsafe.Sizeof(*p))
	maxID := (*pgid)(ptr)
	ptr = unsafeAdd(ptr, unsafe.Sizeof(*maxID))
	minID := (*pgid)(ptr)
	ptr = unsafeAdd(ptr, unsafe.Sizeof(*minID))
	next := (*pgid)(ptr)
	ptr = unsafeAdd(ptr, unsafe.Sizeof(*next))
	size := (*int)(ptr)
	ptr = unsafeAdd(ptr, unsafe.Sizeof(*size))
	var list []pageInfo
	unsafeSlice(unsafe.Pointer(&list), ptr, PageSize/int(unsafe.Sizeof(pageInfo{}))-12)
	return &pageList{
		maxID: *maxID,
		next:  *next,
		list:  list,
	}
}

type node struct {
	maxKey   int
	pgId     pgid
	isBranch bool
	size     int
}

func (n *node) getNewNode(db *DB) *node {
	temp := db.getNewPage()
	tempNode := db.getPage(temp).node()
	db.copyTo(n.pgId, temp)
	return tempNode
}

func (n *node) set(key, value int, db *DB, parent *node) {
	nc := n.getNewNode(db)

	pgId := nc.pgId
	var parentID pgid
	maxKey := n.maxKey
	if parent != nil {
		parentID = parent.pgId
	} else {
		db.getTempMeta().root = pgId
	}
	log.Debugf("set value %v:%v to %v, parent is %v", key, value, pgId, parentID)
	if nc.isBranch {
		values := nc.treeNode().values
		for i := 0; i < nc.size; i++ {
			if values[i].key >= key || i == nc.size-1 {
				childPage := db.getPage(pgid(values[i].value))
				childNode := childPage.node()
				childNode.set(key, value, db, nc)
				break
			}
		}
	} else {
		// 修改数值而不是新增
		itemValue := nc.treeNode().get(key)
		if itemValue.notNull() {
			log.Debugf("just update key:%v value: %v to %v", key, itemValue.value, value)
			itemValue.value = value
			return
		}
		nc.treeNode().add(key, value)
	}
	nn := db.getPage(pgId).node()
	newNode := nn.balance(db)
	nn = db.getPage(pgId).node()
	var parentNode *node
	if parentID != 0 {
		parentNode = db.getPage(parentID).node()
	}

	//分裂

	if newNode != nil {
		//创建parent节点，一般要修改root
		if parentNode == nil {
			maxKey := nc.maxKey
			newParentPageId := db.getNewPage()
			parentPage := db.getPage(newParentPageId)
			parentNode = parentPage.node()
			parentNode.pgId = newParentPageId
			parentNode.isBranch = true
			parentNode.treeNode().add(maxKey, int(pgId))

			dbMeta := db.getTempMeta()
			dbMeta.root = parentNode.pgId
		} else {
			parentTreeNode := parentNode.treeNode()
			maxItem := parentTreeNode.get(maxKey)
			maxItem.key = nn.treeNode().values[nn.size].key
			maxItem.value = int(nn.pgId)
			parentTreeNode.reSort()
		}
		parentTreeNode := parentNode.treeNode()
		parentTreeNode.add(newNode.maxKey, int(newNode.pgId))
	}
	//更新列表数组
	if nn.isBranch {
		vals := nn.treeNode().values
		for i := 0; i < nn.size; i++ {
			val := vals[i]
			childPage := db.getTempPage(pgid(val.value))
			if int(childPage.id) != val.value {
				vals[i].value = int(childPage.id)
			}
			childNode := childPage.node()
			vals[i].key = childNode.maxKey
		}
		nTreeNode := nn.treeNode()
		values := nTreeNode.values
		nn.maxKey = values[nn.size-1].key
	}
}

func (n *node) balance(db *DB) *node {
	id := n.pgId
	if n.size > itemSize {
		log.Debugf("need a new page")
		newPageId := db.getNewPage()
		n := db.getPage(id).node()
		right := db.getPage(newPageId)
		rightNode := right.node()
		rightNode.pgId = newPageId
		rightNode.size = n.size / 2

		n.size = n.size - n.size/2
		n.maxKey = n.treeNode().values[n.size-1].key
		rightValues := rightNode.treeNode().values
		nValues := n.treeNode().values
		for i := 0; i < rightNode.size; i++ {
			rightValues[i] = nValues[n.size+i]
		}
		rightNode.maxKey = rightValues[rightNode.size-1].key
		rightNode.isBranch = n.isBranch
		return rightNode
	}
	return nil
}

func (n *node) delete(key int, db *DB, parent *node) {
	nc := n.getNewNode(db)
	if parent == nil {
		db.getTempMeta().root = nc.pgId
	}
	if nc.isBranch {
		values := nc.treeNode().values
		for i := 0; i < nc.size; i++ {
			if values[i].key >= key {
				childPage := db.getPage(pgid(values[i].value))
				childNode := childPage.node()
				childNode.delete(key, db, nc)
				values[i].value = int(db.getTempPage(pgid(values[i].value)).id)
				break
			}
			if i == nc.size-1 {
				return
			}
		}
		if nc.size < itemSize/2 {
			nc.maxKey = nc.treeNode().values[nc.size-1].key
			nc.itemMoveOrMerge(parent, db)
		}
	} else {
		nc.treeNode().remove(key)
		if nc.size < itemSize/2 {
			nc.itemMoveOrMerge(parent, db)
		}
	}
}

func (n *node) get(key int, db *DB) *item {
	if n.isBranch {
		for _, item := range n.treeNode().values {
			if item.key >= key {
				childPage := db.getPage(pgid(item.value))
				childNode := childPage.node()
				return childNode.get(key, db)
			}
		}
		return &item{math.MinInt, math.MinInt}
	} else {
		return n.treeNode().get(key)
	}
}

func (n *node) itemMoveOrMerge(parent *node, db *DB) {
	var left, right *node
	if parent == nil {
		return
	}
	nt := parent.treeNode()
	index := 0
	for i := 0; i < parent.size; i++ {
		if nt.values[i].key == n.maxKey {
			index = i
			if i-1 > 0 {
				leftPgID := nt.values[i-1].value
				leftPg := db.getPage(pgid(leftPgID))
				left = leftPg.node()
			}
			if i+1 < n.size {
				rightPgID := nt.values[i+1].value
				rightPg := db.getPage(pgid(rightPgID))
				right = rightPg.node()
			}
		}
	}
	if left != nil && left.size > itemSize/2 {
		left = left.getNewNode(db)
		val := left.treeNode().values[left.size-1]
		n.treeNode().add(val.key, val.value)
		left.size--
		left.maxKey = left.treeNode().values[left.size-1].key
		parent.treeNode().values[index-1].key = left.maxKey
		parent.treeNode().values[index-1].value = int(left.pgId)
		return
	}
	if right != nil && right.size > itemSize/2 {
		right = right.getNewNode(db)
		val := right.treeNode().values[0]
		n.treeNode().add(val.key, val.value)
		for i := 0; i < right.size-1; i++ {
			right.treeNode().values[i] = right.treeNode().values[i+1]
		}
		right.size--
		n.maxKey = val.key
		parent.treeNode().values[index].key = n.maxKey
		parent.treeNode().values[index+1].value = int(right.pgId)
		return
	}
	if left != nil && n.size+left.size < itemSize {
		left = left.getNewNode(db)
		nTreeNode := n.treeNode()
		nValues := nTreeNode.values
		for i := 0; i < n.size; i++ {
			val := nValues[i]
			left.treeNode().add(val.key, val.value)
		}
		parent.deleteNode(index)
		parent.treeNode().values[index-1].value = int(left.pgId)
		db.removePage(n.pgId)
		return
	}
	if right != nil && n.size+right.size < itemSize {
		rightTreeNode := right.treeNode()
		rightValues := rightTreeNode.values
		for i := 0; i < right.size; i++ {
			val := rightValues[i]
			n.treeNode().add(val.key, val.value)
		}
		parent.deleteNode(index + 1)
		parent.treeNode().values[index].value = int(n.pgId)
		return
	}
}

func (n *node) treeNode() *bTreeNode {
	var items []item
	ptr := unsafeAdd(unsafe.Pointer(n), unsafe.Sizeof(*n))
	unsafeSlice(unsafe.Pointer(&items), ptr, itemSize+1)
	return &bTreeNode{
		maxKey: n.maxKey,
		node:   n,
		values: items,
	}
}

func (n *node) initTree() {

}

func (n *node) deleteNode(index int) {
	vs := n.treeNode().values
	vs[index-1].key = vs[index].key
	for i := index; i < n.size; i++ {
		vs[i] = vs[i+1]
	}
	n.size -= 1
}

func (p *page) node() *node {
	return (*node)(unsafe.Pointer(uintptr(unsafe.Pointer(p)) + unsafe.Sizeof(*p)))
}
