package memcached

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/bradfitz/gomemcache/memcache"
	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

const (
	memcachedUri        = "memc.uri"
	memcachedUriDefault = "127.0.0.1:11211"
)

type memcached struct {
	cli *memcache.Client
}

func (m *memcached) ToSqlDB() *sql.DB {
	return nil
}

func (m *memcached) Close() error {
	return nil
}

func (m *memcached) InitThread(ctx context.Context, threadID int, threadCount int) context.Context {
	return ctx
}

func (m *memcached) CleanupThread(ctx context.Context) {
}

func (m *memcached) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	it, err := m.cli.Get(m.createQualifiedKey(table, key))
	if err != nil {
		return nil, fmt.Errorf("Read err: %v", err)
	}
	tmpResult := make(map[string][]byte)
	err = json.Unmarshal(it.Value, &tmpResult)
	if err != nil {
		return nil, fmt.Errorf("Read err: %v", err)
	}
	result := make(map[string][]byte)
	for _, field := range fields {
		result[field] = tmpResult[field]
	}
	return result, err
}

func (m *memcached) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	return nil, nil
}

func (m *memcached) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	err := m.set(table, key, values)
	if err != nil {
		return fmt.Errorf("Update err：%v", err)
	}
	return nil
}

func (m *memcached) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	err := m.set(table, key, values)
	if err != nil {
		return fmt.Errorf("Insert err: %v", err)
	}
	return nil
}

func (m *memcached) set(table string, key string, values map[string][]byte) error {
	valJsonBts, err := json.Marshal(values)
	if err != nil {
		return err
	}
	if err = m.cli.Add(&memcache.Item{
		Key:   m.createQualifiedKey(table, key),
		Value: valJsonBts,
	}); err != nil {
		return err
	}
	return nil
}

func (m *memcached) Delete(ctx context.Context, table string, key string) error {
	err := m.cli.Delete(m.createQualifiedKey(table, key))
	if err != nil {
		return fmt.Errorf("Delete err: %v", err)
	}
	return nil
}

func (m *memcached) createQualifiedKey(table, key string) string {
	return fmt.Sprintf("%s-%s", table, key)
}

type memcachedCreator struct {
}

func (c memcachedCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	cli := memcache.New(p.GetString(memcachedUri, memcachedUriDefault))

	m := &memcached{
		cli: cli,
	}

	return m, nil
}

func init() {
	ycsb.RegisterDBCreator("memc", memcachedCreator{})
}
