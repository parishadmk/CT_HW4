package loadbalancer

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/parishadmk/CT_HW4/internal/cluster/controller"
	"github.com/parishadmk/CT_HW4/internal/cluster/etcd"
)

type LoadBalancer struct {
	controllerAddr   string
	httpClient       *http.Client
	ginEngine        *gin.Engine
	etcdStore        *etcd.Store
	metadataLock     sync.RWMutex
	metadataExpiry   time.Time
	refreshInterval  time.Duration
	requestTimeout   time.Duration
	maxRetries       int
	partitionCache   []*controller.PartitionMetadata
	nodeAddressCache map[int]string
}

func NewLoadBalancer(controllerAddr string) *LoadBalancer {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())

	endpoints := []string{"http://etcd:2379"}
	if env := os.Getenv("ETCD_ENDPOINTS"); env != "" {
		endpoints = []string{env}
	}
	store, err := etcd.NewStore(endpoints)
	if err != nil {
		log.Fatalf("failed to connect to etcd: %v", err)
	}

	lb := &LoadBalancer{
		controllerAddr: controllerAddr,
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:       100,
				IdleConnTimeout:    30 * time.Second,
				DisableCompression: false,
			},
		},
		ginEngine:       router,
		refreshInterval: 10 * time.Second,
		requestTimeout:  2 * time.Second,
		maxRetries:      3,
		etcdStore:       store,
	}

	lb.setupRoutes()

	if err := lb.refreshMetadata(); err != nil {
		log.Printf("Initial metadata refresh failed: %v", err)
	}

	go lb.metadataRefresher()

	return lb
}

func (lb *LoadBalancer) getPartitionID(key string) int {
	h := sha256.Sum256([]byte(key))
	return int(h[0]) % len(lb.partitionCache)
}

func (lb *LoadBalancer) getLeaderForPartition(partitionID int) (string, error) {
	lb.metadataLock.RLock()
	defer lb.metadataLock.RUnlock()

	if len(lb.partitionCache) == 0 || time.Now().After(lb.metadataExpiry) {
		return "", errors.New("metadata not available or expired")
	}

	if partitionID >= len(lb.partitionCache) {
		return "", fmt.Errorf("invalid partition ID: %d", partitionID)
	}

	return lb.nodeAddressCache[lb.partitionCache[partitionID].Leader], nil
}

func (lb *LoadBalancer) getReplicaForRead(partitionID int) (string, error) {
	lb.metadataLock.RLock()
	defer lb.metadataLock.RUnlock()

	if len(lb.partitionCache) == 0 || time.Now().After(lb.metadataExpiry) {
		return "", errors.New("metadata not available or expired")
	}

	if partitionID >= len(lb.partitionCache) {
		return "", fmt.Errorf("invalid partition ID: %d", partitionID)
	}

	replicas := lb.partitionCache[partitionID].Replicas
	if len(replicas) == 0 {
		return lb.nodeAddressCache[lb.partitionCache[partitionID].Leader], nil
	}

	return lb.nodeAddressCache[replicas[rand.Intn(len(replicas))]], nil
}

func (lb *LoadBalancer) refreshMetadata() error {
	ctx, cancel := context.WithTimeout(context.Background(), lb.requestTimeout)
	defer cancel()

	nodes, err := lb.etcdStore.ListJSON(ctx, "/nodes/", func() interface{} { return &controller.NodeMetadata{} })
	if err != nil {
		return err
	}
	partitions, err := lb.etcdStore.ListJSON(ctx, "/partitions/", func() interface{} { return &controller.PartitionMetadata{} })
	if err != nil {
		return err
	}

	metadata := struct {
		NodeAddresses map[int]string
		Partitions    []*controller.PartitionMetadata
	}{NodeAddresses: make(map[int]string), Partitions: make([]*controller.PartitionMetadata, len(partitions))}

	for _, nraw := range nodes {
		nm := nraw.(*controller.NodeMetadata)
		metadata.NodeAddresses[nm.ID] = nm.HttpAddress
	}
	for i, praw := range partitions {
		metadata.Partitions[i] = praw.(*controller.PartitionMetadata)
	}

	lb.metadataLock.Lock()
	defer lb.metadataLock.Unlock()

	lb.metadataExpiry = time.Now().Add(lb.refreshInterval)
	lb.nodeAddressCache = metadata.NodeAddresses
	lb.partitionCache = metadata.Partitions

	return nil
}

func (lb *LoadBalancer) metadataRefresher() {
	ticker := time.NewTicker(lb.refreshInterval / 2)
	defer ticker.Stop()

	for range ticker.C {
		if err := lb.refreshMetadata(); err != nil {
			log.Printf("Background metadata refresh failed: %v", err)
		}
	}
}
