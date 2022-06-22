package my_db_code

type branch struct {
	maxKey int
	node   *node
	values []item
}

type item struct {
	key    int
	pgId   pgid
	offset int
}

func (i *item) notNull() bool {
	if i.pgId == 0 && i.offset == 0 && i.key == 0 {
		return false
	}
	return true
}

type pair struct {
	key   int
	value int
}

type leaf struct {
	maxKey int
	node   *node
	pairs  []pair
}

func (l *leaf) add(key int, value int) {
	l.pairs[l.node.size].key = key
	l.pairs[l.node.size].value = value
	l.node.size++
}

func (l *leaf) update(offset, value int) {
	l.pairs[offset].value = value
}

func (l *leaf) get(offset int) int {
	if offset > l.node.size {
		return -1
	}
	return l.pairs[offset].value
}

func (b *branch) add(key, val int) {
	b.values[b.node.size].key = key
	b.values[b.node.size].pgId = initLeafPageId
	b.values[b.node.size].offset = b.node.size
	b.node.size++
}

func (b *branch) get(key int) item {
	for i := 0; i < b.node.size; i++ {
		if b.values[i].key == key {
			return b.values[i]
		}
	}
	return item{}
}
