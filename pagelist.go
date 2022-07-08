package my_db_code

import (
	"math"
	"unsafe"
)

const PageSize = 4 * 1024
const itemSize = 252

type pgid int32

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
	if n.isBranch {
		values := n.treeNode().values
		for i := 0; i < n.size; i++ {
			if values[i].key >= key || i == n.size-1 {
				childPage := db.pageInBuffer(db.dataRef, pgid(values[i].value))
				childNode := childPage.node()
				childNode.set(key, value, db, n)
				//if key>values[i].key{
				//	values[i].key=key
				//	n.treeNode().reSort()
				//}
				break
			}
		}
	} else {
		// 修改数值而不是新增
		itemValue := n.treeNode().get(key)
		if itemValue.notNull() {
			itemValue.value = value
			return
		}
		n.treeNode().add(key, value)
	}
	newNode := n.balance(db)
	//分裂
	if newNode != nil {
		//创建parent节点，一般要修改root
		if parent == nil {
			newParentPageId := db.getNewPage()
			parentPage := db.pageInBuffer(db.dataRef, newParentPageId)
			parent = parentPage.node()
			parent.pgId = newParentPageId
			parent.isBranch = true
			parent.treeNode().add(n.maxKey, int(n.pgId))

			metaPage := db.pageInBuffer(db.dataRef, initMetaPageId)
			dbMeta := metaPage.meta()
			dbMeta.root = parent.pgId
		} else {
			parentTreeNode := parent.treeNode()
			item := parentTreeNode.get(n.maxKey)
			item.key = n.treeNode().values[n.size/2].key
			parentTreeNode.reSort()
		}
		parentTreeNode := parent.treeNode()
		parentTreeNode.add(newNode.maxKey, int(newNode.pgId))
	}
	//更新列表数组
	if n.isBranch {
		for i := 0; i < n.size; i++ {
			value := n.treeNode().values[i]
			childPage := db.pageInBuffer(db.dataRef, pgid(value.value))
			childNode := childPage.node()
			value.key = childNode.maxKey
		}
		nTreeNode := n.treeNode()
		values := nTreeNode.values
		n.maxKey = values[n.size-1].key
	}
}

func (n *node) balance(db *DB) *node {
	if n.size > itemSize {
		newPageId := db.getNewPage()
		right := db.pageInBuffer(db.dataRef, newPageId)
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

func (n *node) get(key int, db *DB) *item {
	if n.isBranch {
		for _, item := range n.treeNode().values {
			if item.key >= key {
				childPage := db.pageInBuffer(db.dataRef, pgid(item.value))
				childNode := childPage.node()
				return childNode.get(key, db)
			}
		}
		return &item{math.MinInt, math.MinInt}
	} else {
		return n.treeNode().get(key)
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
