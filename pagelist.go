package my_db_code

import (
	"unsafe"
)

const PageSize = 4 * 1024

type pgid uint64

const (
	metaPageType     = 0
	freelistPageType = 1
	branchPageType   = 2
	leafPageType     = 3
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

func (n *node) set(key, value int) {
	if n.isBranch {
		n.branch().add(key, value)
	} else {
		n.leaf().add(key, value)
	}
}

func (n *node) get(key int) *item {
	i := n.branch().get(key)
	return i
}

func (n *node) branch() *branch {
	var items []item
	ptr := unsafeAdd(unsafe.Pointer(n), unsafe.Sizeof(*n))
	unsafeSlice(unsafe.Pointer(&items), ptr, int((uintptr(PageSize)-unsafe.Sizeof(*n))/unsafe.Sizeof(item{})))
	return &branch{
		maxKey: n.maxKey,
		node:   n,
		values: items,
	}
}

func (n *node) leaf() *leaf {
	var pairs []pair
	ptr := unsafeAdd(unsafe.Pointer(n), unsafe.Sizeof(*n))
	unsafeSlice(unsafe.Pointer(&pairs), ptr, 1000)
	return &leaf{
		maxKey: n.maxKey,
		node:   n,
		pairs:  pairs,
	}
}

func (p *page) node() *node {
	return (*node)(unsafe.Pointer(uintptr(unsafe.Pointer(p)) + unsafe.Sizeof(*p)))
}
