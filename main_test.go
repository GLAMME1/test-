package main

import (
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/restream/reindexer"
)

func TestMain(m *testing.M) {
	// init() из main.go запускается сам, до TestMain
	// Закрываем коннект к базе после всех тестов
	defer db.Close() 

	m.Run()
	
	log.Println("Тесты финишировали. Reindexer отключен")
}

func TestReindexerService(t *testing.T) {
	assert := assert.New(t)

	// Чистим базу перед каждым тестом, чтобы они не мешали друг другу
	_ = db.DropNamespace("documents")
	// Заново открываем коллекцию после удаления
	if err := db.OpenNamespace("documents", reindexer.DefaultNamespaceOptions(), Document{}); err != nil {
		log.Fatalf("Ошибка при открытии/создании коллекции перед тестами: %v", err)
	}

	// 1. Создаем документ
	doc1 := Document{
		ID:   1,
		Name: "Тестовый Документ 1",
		Desc: "Это описание первого тестового документа",
		SubDocsLevel1: []SubDocument{
			{
				Field1: "sub1_1", Field2: 10, Sort: 3,
				SubDocsLevel2: []SubDocumentLevel2{
					{Field1: "sub2_1_1", Field2: 100},
					{Field1: "sub2_1_2", Field2: 200},
				},
			},
			{
				Field1: "sub1_2", Field2: 20, Sort: 1,
				SubDocsLevel2: []SubDocumentLevel2{
					{Field1: "sub2_2_1", Field2: 300},
				},
			},
			{
				Field1: "sub1_3", Field2: 30, Sort: 2,
				SubDocsLevel2: []SubDocumentLevel2{
					{Field1: "sub2_3_1", Field2: 400},
					{Field1: "sub2_3_2", Field2: 500},
					{Field1: "sub2_3_3", Field2: 600},
				},
			},
		},
	}

	err := CreateDocument(doc1)
	assert.NoError(err, "Должно быть без ошибок при создании")
	log.Println("Документ 1 создан")

	// 2. Читаем документ
	retrievedDoc, err := GetDocument(1)
	assert.NoError(err, "Должно быть без ошибок при чтении")
	assert.NotNil(retrievedDoc, "Документ не должен быть пустым")
	assert.Equal(doc1.ID, retrievedDoc.ID, "ID должен совпадать")
	assert.Equal(doc1.Name, retrievedDoc.Name, "Имя должно совпадать")
	assert.Equal(doc1.Desc, retrievedDoc.Desc, "Описание должно совпадать")
	log.Printf("Прочитали документ: %+v\n", *retrievedDoc)

	// Проверяем сортировку и обработку полей
	ProcessAndSortDocument(retrievedDoc)
	assert.Equal(3, retrievedDoc.SubDocsLevel1[0].Sort, "Первый элемент должен быть с Sort 3")
	assert.Equal(2, retrievedDoc.SubDocsLevel1[1].Sort, "Второй элемент должен быть с Sort 2")
	assert.Equal(1, retrievedDoc.SubDocsLevel1[2].Sort, "Третий элемент должен быть с Sort 1")
	// Проверяем, что Field2 обнулился во втором уровне вложенности
	assert.Equal(0, retrievedDoc.SubDocsLevel1[0].SubDocsLevel2[0].Field2, "Field2 должен быть 0")
	assert.Equal(0, retrievedDoc.SubDocsLevel1[0].SubDocsLevel2[1].Field2, "Field2 должен быть 0")
	assert.Equal(0, retrievedDoc.SubDocsLevel1[1].SubDocsLevel2[0].Field2, "Field2 должен быть 0")
	log.Printf("Обработанный и отсортированный: %+v\n", *retrievedDoc)

	// 3. Обновляем документ
	doc1.Name = "Обновленный Документ 1"
	doc1.Desc = "Обновленное описание"
	err = UpdateDocument(doc1)
	assert.NoError(err, "Должно быть без ошибок при обновлении")
	log.Println("Документ 1 обновлен")

	retrievedDoc, err = GetDocument(1) // Кэш должен промахнуться, берем из базы
	assert.NoError(err, "Должно быть без ошибок при чтении обновленного")
	assert.NotNil(retrievedDoc, "Обновленный документ не должен быть пустым")
	assert.Equal("Обновленный Документ 1", retrievedDoc.Name, "Имя должно обновиться")
	assert.Equal("Обновленное описание", retrievedDoc.Desc, "Описание должно обновиться")
	log.Printf("Прочитали обновленный: %+v\n", *retrievedDoc)

	// 4. Проверяем список документов с пагинацией
	doc2 := Document{
		ID:   2,
		Name: "Тестовый Документ 2",
		Desc: "Это описание второго тестового документа",
		SubDocsLevel1: []SubDocument{
			{
				Field1: "sub1_A", Field2: 15, Sort: 5,
				SubDocsLevel2: []SubDocumentLevel2{
					{Field1: "sub2_A_1", Field2: 700},
				},
			},
			{
				Field1: "sub1_B", Field2: 25, Sort: 4,
				SubDocsLevel2: []SubDocumentLevel2{
					{Field1: "sub2_B_1", Field2: 800},
					{Field1: "sub2_B_2", Field2: 900},
				},
			},
		},
	}
	err = CreateDocument(doc2)
	assert.NoError(err, "Должно быть без ошибок при создании второго документа")
	log.Println("Документ 2 создан")

	listedDocs, err := ListDocuments(0, 1)
	assert.NoError(err, "Должно быть без ошибок при получении списка")
	assert.Len(listedDocs, 1, "Должен быть 1 документ")
	// Порядок может быть непредсказуемым после ProcessAndSortDocument, так что проверяем только наличие
	assert.NotNil(listedDocs[0], "Документ в списке не должен быть пустым")
	log.Printf("Список документов: %+v\n", listedDocs)

	// 5. Удаляем документ
	err = DeleteDocument(1)
	assert.NoError(err, "Должно быть без ошибок при удалении")
	log.Println("Документ 1 удален")

	retrievedDoc, err = GetDocument(1) // Пытаемся получить удаленный
	assert.Error(err, "Должна быть ошибка при попытке получить удаленный")
	assert.Nil(retrievedDoc, "Удаленный документ должен быть пустым")
	log.Println("Документ 1 не найден (как и ожидалось)")
}
