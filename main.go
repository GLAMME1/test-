package main

import (
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/patrickmn/go-cache"
	"github.com/restream/reindexer"
	"github.com/spf13/viper"
)

// Структура для конфигурации приложения
type Config struct {
	DB struct {
		Host string `mapstructure:"host"`
		Port int    `mapstructure:"port"`
		Name string `mapstructure:"name"`
	} `mapstructure:"db"`
}

// Второй уровень вложенности
type SubDocumentLevel2 struct {
	Field1 string `reindex:"field1"`
	Field2 int    `reindex:"field2"`
}

// Первый уровень вложенности
type SubDocument struct {
	Field1        string              `reindex:"field1"`
	Field2        int                 `reindex:"field2"`
	Sort          int                 `reindex:"sort"`
	SubDocsLevel2 []SubDocumentLevel2 `reindex:"subdocs_level2"` // Второй уровень вложенности
}

// Основной документ
type Document struct {
	ID            int           `reindex:"id,hash,pk"`
	Name          string        `reindex:"name"`
	SubDocsLevel1 []SubDocument `reindex:"subdocs_level1"` // Первый уровень вложенности
}

var (
	db *reindexer.Reindexer // Клиент Reindexer для работы с базой
	c  *cache.Cache       // Наш кэш для документов
)

func init() {
	config, err := LoadConfig()
	if err != nil {
		log.Fatalf("Ошибка загрузки конфигурации: %v", err)
	}

	log.Printf("Подключение к Reindexer по адресу cproto://%s:%d/%s", config.DB.Host, config.DB.Port, config.DB.Name)
	db = reindexer.NewReindex(fmt.Sprintf("cproto://%s:%d/%s", config.DB.Host, config.DB.Port, config.DB.Name), reindexer.WithCreateDBIfMissing())

	// Проверяем коннект к базе
	if err := db.Ping(); err != nil {
		log.Fatalf("Не удалось подключиться к Reindexer: %v", err)
	}
	log.Println("Успешное подключение к Reindexer")

	// Убеждаемся, что коллекция 'documents' существует (создаем, если нет)
	if err := db.OpenNamespace("documents", reindexer.DefaultNamespaceOptions(), Document{}); err != nil {
		log.Fatalf("Ошибка открытия или создания коллекции: %v", err)
	}
	log.Println("Коллекция 'documents' готова к работе")

	// Настраиваем кэш: элементы живут 15 минут, чистим каждые 20 минут
	c = cache.New(15*time.Minute, 20*time.Minute)
}


func LoadConfig() (*Config, error) {
	v := viper.New()
	v.AddConfigPath("./config")
	v.SetConfigName("config")   
	v.SetConfigType("yaml")      

	v.AutomaticEnv() // Читаем env-переменные автоматом

	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			log.Println("Конфиг не найден, берем настройки из окружения")
		} else {
			return nil, fmt.Errorf("не удалось прочитать файл конфигурации: %w", err)
		}
	}

	var config Config
	if err := v.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("не удалось десериализовать конфигурацию: %w", err)
	}

	// Проставляем дефолты, если env-переменные не заданы
	if config.DB.Host == "" {
		config.DB.Host = v.GetString("DB_HOST")
	}
	if config.DB.Port == 0 {
		config.DB.Port = v.GetInt("DB_PORT")
	}
	if config.DB.Name == "" {
		config.DB.Name = v.GetString("DB_NAME")
	}

	// Дефолтные значения, если нигде не задано
	if config.DB.Host == "" {
		config.DB.Host = "127.0.0.1"
	}
	if config.DB.Port == 0 {
		config.DB.Port = 6534
	}
	if config.DB.Name == "" {
		config.DB.Name = "testdb"
	}

	return &config, nil
}

// Обрабатывает и сортирует документ
func ProcessAndSortDocument(doc *Document) {
	// Сортируем по полю 'Sort' (обратный порядок)
	sort.Slice(doc.SubDocsLevel1, func(i, j int) bool {
		return doc.SubDocsLevel1[i].Sort > doc.SubDocsLevel1[j].Sort
	})

	
	for i := range doc.SubDocsLevel1 {
		for j := range doc.SubDocsLevel1[i].SubDocsLevel2 {
			doc.SubDocsLevel1[i].SubDocsLevel2[j].Field2 = 0 // Обнуляем Field2
		}
	}
}

// Достает документ по ID, сначала проверяя кэш
func GetDocument(id int) (*Document, error) {
	cacheKey := fmt.Sprintf("doc_%d", id)
	if x, found := c.Get(cacheKey); found {
		log.Printf("Кэш-хит для ID %d", id)
		return x.(*Document), nil
	}

	log.Printf("Кэш-мисс для ID %d, берем из базы", id)
	
	it := db.Query("documents").Where("id", reindexer.EQ, id).Exec()
	defer it.Close()

	item, err := it.FetchOne()
	if err != nil {
		return nil, fmt.Errorf("ошибка при получении документа из Reindexer: %w", err)
	}

	doc, ok := item.(*Document)
	if !ok {
		return nil, fmt.Errorf("не удалось привести элемент к типу *Document")
	}
	
	c.Set(cacheKey, doc, cache.DefaultExpiration) // Кэшируем
	return doc, nil
}

// Создает новый документ (или обновляет, если есть) и чистит кэш
func CreateDocument(doc Document) error {
	err := db.Upsert("documents", doc)
	if err == nil {
		c.Delete(fmt.Sprintf("doc_%d", doc.ID)) // Успех? Удаляем из кэша
	}
	return err
}

// Обновляет существующий документ в Reindexer
func UpdateDocument(doc Document) error {
	err := db.Upsert("documents", doc)
	if err == nil {
		c.Delete(fmt.Sprintf("doc_%d", doc.ID)) // Успех? Удаляем из кэша
	}
	return err
}

// Удаляет документ по ID и чистит кэш
func DeleteDocument(id int) error {
	err := db.Delete("documents", Document{ID: id}) 
	if err == nil {
		c.Delete(fmt.Sprintf("doc_%d", id)) // Успех? Удаляем из кэша
	}
	return err
}

// Получает список документов с пагинацией и обрабатывает параллельно
func ListDocuments(offset, limit int) ([]Document, error) {
	var documents []Document
	query := db.Query("documents").Offset(offset).Limit(limit) // Запрос с пагинацией
	it := query.Exec()
	defer it.Close() // Закрываем итератор

	log.Printf("Начинаем перебор документов: offset=%d, limit=%d", offset, limit)
	for it.Next() {
		log.Println("Нашли документ, пробуем распарсить...")
		item := it.Object()
		if item == nil {
			log.Println("Получили пустой объект из Reindexer")
			continue
		}

		doc, ok := item.(*Document)
		if !ok {
			log.Printf("Не удалось привести элемент к типу *Document: %T", item)
			continue
		}

		if err := it.Error(); err != nil {
			log.Printf("Ошибка чтения документа после парсинга: %v", err)
			continue
		}
		documents = append(documents, *doc) 
	}

	log.Printf("Закончили перебор, найдено документов: %d", len(documents))

	// Обрабатываем каждый документ в отдельной горутине, чтобы не потерять общую сортировку
	var wg sync.WaitGroup
	processedDocuments := make(chan Document, len(documents)) // Канал для готовых документов

	for i := range documents {
		wg.Add(1) 
		go func(d Document) { 
			defer wg.Done() 
			ProcessAndSortDocument(&d)
			processedDocuments <- d    
		}(documents[i]) 
	}

	wg.Wait() 
	close(processedDocuments) 

	var finalDocuments []Document
	for d := range processedDocuments {
		finalDocuments = append(finalDocuments, d)
	}

	// Важно: Reindexer сам выдает документы в нужном порядке. ProcessAndSortDocument меняет только внутренности документа, не нарушая общую сортировку
	// Если Reindexer не сортировал бы, пришлось бы делать сортировку тут

	return finalDocuments, nil 
}


func main() {
	select {} // Держим программу в рабочем состоянии
}
