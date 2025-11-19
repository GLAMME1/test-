package service

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/patrickmn/go-cache"

	"reindexer-service/internal/models"
	"reindexer-service/internal/reindexer"
	rawreindexer "github.com/restream/reindexer"
)

type Service struct {
	db    *reindexer.Client
	cache *cache.Cache
}

func New(db *reindexer.Client) *Service {
	return &Service{
		db:    db,
		cache: cache.New(15*time.Minute, 20*time.Minute),
	}
}

func (s *Service) EnsureCollections() error {
	return s.db.EnsureNamespace("documents")
}

func (s *Service) GetDoc(ctx context.Context, id int) (*models.Document, error) {
	key := fmt.Sprintf("doc_%d", id)
	if v, found := s.cache.Get(key); found {
		if doc, ok := v.(*models.Document); ok {
			log.Printf("cache hit: id=%d", id)
			return doc, nil
		}
	}
	log.Printf("cache miss, loading from db: id=%d", id)
	it := s.db.DB().Query("documents").Where("id", rawreindexer.EQ, id).Exec()
	defer it.Close()
	item, err := it.FetchOne()
	if err != nil {
		return nil, fmt.Errorf("fetch doc %d: %w", id, err)
	}
	doc, ok := item.(*models.Document)
	if !ok {
		return nil, fmt.Errorf("wrong type from db for doc %d", id)
	}
	s.cache.Set(key, doc, cache.DefaultExpiration)
	return doc, nil
}

func (s *Service) CreateOrUpdateDoc(ctx context.Context, doc models.Document) error {
	err := s.db.DB().Upsert("documents", doc)
	if err == nil {
		s.cache.Delete(fmt.Sprintf("doc_%d", doc.ID))
		log.Printf("document upserted: id=%d", doc.ID)
	}
	return err
}

func (s *Service) DeleteDoc(ctx context.Context, id int) error {
	err := s.db.DB().Delete("documents", models.Document{ID: id})
	if err == nil {
		s.cache.Delete(fmt.Sprintf("doc_%d", id))
		log.Printf("document deleted: id=%d", id)
	}
	return err
}

func (s *Service) ListDocs(ctx context.Context, offset, limit int) ([]models.Document, error) {
	var docs []models.Document
	it := s.db.DB().Query("documents").Offset(offset).Limit(limit).Exec()
	defer it.Close()
	for it.Next() {
		item := it.Object()
		if doc, ok := item.(*models.Document); ok {
			docs = append(docs, *doc)
		}
	}
	if it.Error() != nil {
		return nil, fmt.Errorf("list docs query: %w", it.Error())
	}
	// обработка и сортировка в параллели
	return processAndSortDocs(docs), nil
}

func processAndSortDocs(docs []models.Document) []models.Document {
	// параллельная обработка без нарушения порядка
	out := make([]models.Document, len(docs))
	wg := sync.WaitGroup{}
	for i := range docs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			processDoc(&docs[i])
			out[i] = docs[i]
		}(i)
	}
	wg.Wait()
	return out
}

func processDoc(doc *models.Document) {
	// Сортируем по полю Sort (обратный порядок)
	entries := doc.SubDocsLevel1
	// manual sort по убыванию
	for i := 0; i < len(entries); i++ {
		for j := i + 1; j < len(entries); j++ {
			if entries[i].Sort < entries[j].Sort {
				entries[i], entries[j] = entries[j], entries[i]
			}
		}
	}
	// исключаем поля: например, зануляем Field2
	for i := range entries {
		for j := range entries[i].SubDocsLevel2 {
			entries[i].SubDocsLevel2[j].Field2 = 0
		}
	}
	doc.SubDocsLevel1 = entries
}
