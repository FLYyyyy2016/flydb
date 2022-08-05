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

func (b *bTreeNode) add(key, val int) {
	b.values[b.node.size].key = key
	b.values[b.node.size].value = val
	b.node.size++
	b.reSort()
	b.maxKey = b.values[b.node.size-1].key
	b.node.maxKey = b.maxKey
}

func (b *bTreeNode) remove(key int) {
	find := false
	for i := 0; i < b.node.size; i++ {
		if b.values[i].key == key {
			find = true
		} else if find {
			b.values[i] = b.values[i+1]
		}
	}
	if find {
		b.node.size--
		b.maxKey = b.values[b.node.size-1].key
		b.node.maxKey = b.maxKey
	}
}

func (b *bTreeNode) reSort() {
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
		return b.getKeyByRange(key, start, mid-1)
	} else {
		return b.getKeyByRange(key, mid+1, end)
	}
}
