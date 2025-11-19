package reindexer

import (
	"fmt"
	"github.com/restream/reindexer"

	"reindexer-service/internal/config"
	"reindexer-service/internal/models"
)

type Client struct {
	db     *reindexer.Reindexer
}

func New(dbcfg config.DB) (*Client, error) {
	dsn := fmt.Sprintf("cproto://%s:%d/%s", dbcfg.Host, dbcfg.Port, dbcfg.Name)
	db := reindexer.NewReindex(dsn, reindexer.WithCreateDBIfMissing())
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("reindexer ping: %w", err)
	}
	return &Client{db: db}, nil
}

func (c *Client) EnsureNamespace(ns string) error {
	return c.db.OpenNamespace(ns, reindexer.DefaultNamespaceOptions(), models.Document{})
}

func (c *Client) Close() { c.db.Close() }

func (c *Client) DB() *reindexer.Reindexer {
	return c.db
}

