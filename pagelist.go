package my_db_code

import (
	log "github.com/sirupsen/logrus"
	"math"
	"unsafe"
)

const PageSize = 4 * 1024
const itemSize = 150

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

func (p *page) meta() *meta {
	return (*meta)(unsafe.Pointer(uintptr(unsafe.Pointer(p)) + unsafe.Sizeof(*p)))
}

type node struct {
	maxKey   int
	pgId     pgid
	isBranch bool
	size     int
}

func (n *node) set(key, value int, db *DB, parent *node) {
	pgId := n.pgId
	var parentID pgid
	if parent != nil {
		parentID = parent.pgId
	}
	log.Debugf("set value %v:%v to %v, parent is %v", key, value, pgId, parentID)
	if n.isBranch {
		values := n.treeNode().values
		for i := 0; i < n.size; i++ {
			if values[i].key >= key || i == n.size-1 {
				childPage := db.getPage(pgid(values[i].value))
				childNode := childPage.node()
				childNode.set(key, value, db, n)
				break
			}
		}
	} else {
		// 修改数值而不是新增
		itemValue := n.treeNode().get(key)
		if itemValue.notNull() {
			log.Debugf("just update key:%v value: %v to %v", key, itemValue.value, value)
			itemValue.value = value
			return
		}
		n.treeNode().add(key, value)
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
			maxKey := n.maxKey
			newParentPageId := db.getNewPage()
			parentPage := db.getPage(newParentPageId)
			parentNode = parentPage.node()
			parentNode.pgId = newParentPageId
			parentNode.isBranch = true
			parentNode.treeNode().add(maxKey, int(pgId))

			dbMeta := db.getMeta()
			dbMeta.root = parentNode.pgId
		} else {
			parentTreeNode := parentNode.treeNode()
			maxItem := parentTreeNode.get(newNode.maxKey)
			maxItem.key = nn.treeNode().values[nn.size].key
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
			childPage := db.getPage(pgid(val.value))
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
		rightNode.size = n.size - n.size/2

		n.size = n.size / 2
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
	if n.isBranch {
		values := n.treeNode().values
		for i := 0; i < n.size; i++ {
			if values[i].key >= key || i == n.size-1 {
				childPage := db.getPage(pgid(values[i].value))
				childNode := childPage.node()
				childNode.delete(key, db, n)
				break
			}
		}
		if n.size < itemSize/2 {
			n.maxKey = n.treeNode().values[n.size-1].key
			n.itemMoveOrMerge(parent, db)
		}
	} else {
		n.treeNode().remove(key)
		if n.size < itemSize/2 {
			n.itemMoveOrMerge(parent, db)
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
		if nt.values[i].value == int(n.pgId) {
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
		val := left.treeNode().values[left.size-1]
		n.treeNode().add(val.key, val.value)
		left.size--
		left.maxKey = left.treeNode().values[left.size-1].key
		parent.treeNode().values[index-1].key = left.maxKey
		return
	}
	if right != nil && right.size > itemSize/2 {
		val := right.treeNode().values[0]
		n.treeNode().add(val.key, val.value)
		for i := 0; i < right.size-1; i++ {
			right.treeNode().values[i] = right.treeNode().values[i+1]
		}
		right.size--
		n.maxKey = val.key
		parent.treeNode().values[index].key = n.maxKey
		return
	}
	if left != nil && n.size+left.size < itemSize {
		nTreeNode := n.treeNode()
		nValues := nTreeNode.values
		for i := 0; i < n.size; i++ {
			val := nValues[i]
			left.treeNode().add(val.key, val.value)
		}
		parent.deleteNode(index)
		db.removePage(n.pgId)
		return
	}
	if left != nil && n.size+right.size < itemSize {
		rightTreeNode := right.treeNode()
		rightValues := rightTreeNode.values
		for i := 0; i < left.size; i++ {
			val := rightValues[i]
			n.treeNode().add(val.key, val.value)
		}
		parent.deleteNode(index + 1)
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

//func (n *node) leaf() *leaf {
//	var pairs []pair
//	ptr := unsafeAdd(unsafe.Pointer(n), unsafe.Sizeof(*n))
//	unsafeSlice(unsafe.Pointer(&pairs), ptr, 1000)
//	return &leaf{
//		maxKey: n.maxKey,
//		node:   n,
//		pairs:  pairs,
//	}
//}

func (p *page) node() *node {
	return (*node)(unsafe.Pointer(uintptr(unsafe.Pointer(p)) + unsafe.Sizeof(*p)))
}
