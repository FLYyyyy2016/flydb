package my_db_code

const PageSize = 4 * 1024

type PageFlag int

const (
	MetaPageFlag PageFlag = 0
	FreeListPageFlag
	DataPageFlag
)

type Page struct {
	pgId     int
	pageFlag PageFlag
	data     []byte
}

func (p *Page) LoadData() {

}
