package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"reindexer-service/internal/config"
	"reindexer-service/internal/reindexer"
	"reindexer-service/internal/service"
)

func main() {
	defer log.Println("shutdown")

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config load error: %v", err)
	}
	db, err := reindexer.New(cfg.DB)
	if err != nil {
		log.Fatalf("reindexer init error: %v", err)
	}
	defer db.Close()
	srv := service.New(db)
	if err := srv.EnsureCollections(); err != nil {
		log.Fatalf("collections ensure error: %v", err)
	}
	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	log.Println("reidx service up")
	<-quit
	log.Println("shutdown")
}
