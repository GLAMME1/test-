package models

type SubDocumentLevel2 struct {
	Field1 string `reindex:"field1"`
	Field2 int    `reindex:"field2"`
}

type SubDocument struct {
	Field1        string              `reindex:"field1"`
	Field2        int                 `reindex:"field2"`
	Sort          int                 `reindex:"sort"`
	SubDocsLevel2 []SubDocumentLevel2 `reindex:"subdocs_level2"`
}

type Document struct {
	ID            int           `reindex:"id,hash,pk"`
	Name          string        `reindex:"name"`
	SubDocsLevel1 []SubDocument `reindex:"subdocs_level1"`
}
