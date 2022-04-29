package my_db_code

import "unsafe"

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
	freelist int
	root     int
}

func (p *page) meta() *meta {
	return (*meta)(unsafe.Pointer(uintptr(unsafe.Pointer(p)) + unsafe.Sizeof(*p)))
}

type node struct {
	maxKey   int
	pgId     pgid
	isBranch bool
}

func (n *node) set(key, value int) {

}

func (p *page) node() *node {
	return (*node)(unsafe.Pointer(uintptr(unsafe.Pointer(p)) + unsafe.Sizeof(*p)))
}
