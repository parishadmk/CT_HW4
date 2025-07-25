package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Store wraps an etcd client used for cluster metadata.
type Store struct {
	Cli *clientv3.Client
}

// NewStore creates a Store with the given endpoints.
func NewStore(endpoints []string) (*Store, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	return &Store{Cli: cli}, nil
}

// PutJSON marshals v and stores it under key.
func (s *Store) PutJSON(ctx context.Context, key string, v interface{}) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = s.Cli.Put(ctx, key, string(data))
	return err
}

// GetJSON retrieves key and unmarshals JSON into v.
func (s *Store) GetJSON(ctx context.Context, key string, v interface{}) error {
	resp, err := s.Cli.Get(ctx, key)
	if err != nil {
		return err
	}
	if len(resp.Kvs) == 0 {
		return fmt.Errorf("key not found")
	}
	return json.Unmarshal(resp.Kvs[0].Value, v)
}

// ListJSON lists keys with prefix and unmarshals each into vCreator()
// returning slice of the created values.
func (s *Store) ListJSON(ctx context.Context, prefix string, vCreator func() interface{}) ([]interface{}, error) {
	resp, err := s.Cli.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	result := make([]interface{}, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		v := vCreator()
		if err := json.Unmarshal(kv.Value, v); err != nil {
			return nil, err
		}
		result = append(result, v)
	}
	return result, nil
}

// RegisterWithLease writes key -> v using a lease with ttl seconds.
// Caller should keep the lease alive.
func (s *Store) RegisterWithLease(ctx context.Context, key string, v interface{}, ttl int64) (clientv3.LeaseID, error) {
	lease, err := s.Cli.Grant(ctx, ttl)
	if err != nil {
		return 0, err
	}
	data, err := json.Marshal(v)
	if err != nil {
		return 0, err
	}
	if _, err := s.Cli.Put(ctx, key, string(data), clientv3.WithLease(lease.ID)); err != nil {
		return 0, err
	}
	return lease.ID, nil
}

// KeepAliveLoop keeps the given lease alive until ctx is done.
func (s *Store) KeepAliveLoop(ctx context.Context, leaseID clientv3.LeaseID) error {
	ch, err := s.Cli.KeepAlive(ctx, leaseID)
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case _, ok := <-ch:
			if !ok {
				return nil
			}
		}
	}
}
