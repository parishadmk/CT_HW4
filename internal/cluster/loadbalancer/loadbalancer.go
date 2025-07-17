package loadbalancer

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/Kafsh-e-Mardane-Varzeshi-Hypo-Test-Team/CT_HW3/internal/cluster/controller"
	"github.com/gin-gonic/gin"
)

type LoadBalancer struct {
	controllerAddr   string
	httpClient       *http.Client
	ginEngine        *gin.Engine
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

	url := fmt.Sprintf("%s/metadata", lb.controllerAddr)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return err
	}

	resp, err := lb.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to fetch metadata: status %d", resp.StatusCode)
	}

	metadata := struct {
		NodeAddresses map[int]string                  `json:"nodes"`
		Partitions    []*controller.PartitionMetadata `json:"partitions"`
	}{}
	if err := json.NewDecoder(resp.Body).Decode(&metadata); err != nil {
		return err
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
