package my_db_code

import (
	"math"
	"sort"
)

type bTreeNode struct {
	maxKey int
	node   *node
	values items
}

type item struct {
	key   int
	value int
}

type items []item

func (i items) Len() int {
	return len(i)
}
func (i items) Less(a, b int) bool {
	return i[a].key < i[b].key
}
func (i items) Swap(a, b int) {
	i[a].key, i[b].key = i[b].key, i[a].key
	i[a].value, i[b].value = i[b].value, i[a].value
}

func (i *item) notNull() bool {
	if i.key == math.MinInt && i.value == math.MinInt {
		return false
	}
	return true
}

//
//type pair struct {
//	key   int
//	value int
//}
//
//type pairs []pair
//
//func (i pairs) Len() int {
//	return len(i)
//}
//func (i pairs) Less(a, b int) bool {
//	return i[a].key < i[b].key
//}
//func (i pairs) Swap(a, b int) {
//	i[a].key, i[b].key = i[b].key, i[a].key
//	i[a].value, i[b].value = i[b].value, i[a].value
//}
//
//type leaf struct {
//	maxKey int
//	node   *node
//	pairs  pairs
//}
//
//func (l *leaf) add(key int, value int) {
//	l.pairs[l.node.size].key = key
//	l.pairs[l.node.size].value = value
//	l.node.size++
//	sort.Sort(l.pairs[0:l.node.size])
//}
//
//func (l *leaf) update(offset, value int) {
//	l.pairs[offset].value = value
//}
//
//func (l *leaf) get(offset int) int {
//	if offset > l.node.size {
//		return -1
//	}
//	return l.pairs[offset].value
//}

func (b *bTreeNode) add(key, val int) {
	b.values[b.node.size].key = key
	b.values[b.node.size].value = val
	b.node.size++
	sort.Sort(b.values[0:b.node.size])
}

func (b *bTreeNode) get(key int) *item {
	return b.getKeyByRange(key, 0, b.node.size-1)
}

func (b *bTreeNode) getKeyByRange(key, start, end int) *item {
	mid := (start + end) / 2
	if start > end {
		return &item{math.MinInt, math.MinInt}
	}
	if key == b.values[mid].key {
		return &b.values[mid]
	} else if key < b.values[mid].key {
		return b.getKeyByRange(key, start, mid)
	} else {
		return b.getKeyByRange(key, mid+1, end)
	}
}
