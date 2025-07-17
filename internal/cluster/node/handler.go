package node

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strconv"

	"github.com/Kafsh-e-Mardane-Varzeshi-Hypo-Test-Team/CT_HW3/internal/cluster/replica"
	"github.com/gin-gonic/gin"
)

func (n *Node) setupRoutes() {
	// controller routes
	n.ginEngine.POST("/add-partition/:partition-id", n.handleAddPartition)
	n.ginEngine.POST("/set-leader/:partition-id", n.handleSetLeader)
	n.ginEngine.POST("/set-follower/:partition-id", n.handleSetFollower)
	n.ginEngine.DELETE("/delete-partition/:partition-id", n.handleDeletePartition)
	n.ginEngine.POST("/send-partition/:partition-id/:address", n.handleSendPartitionToNode)

	// loadbalancer routes
	n.ginEngine.POST("/:partition-id/:key/:value", n.handleSetRequest)
	n.ginEngine.GET("/:partition-id/:key", n.handleGetRequest)
	n.ginEngine.DELETE("/:partition-id/:key", n.handleDeleteRequest)
}

func (n *Node) handleAddPartition(c *gin.Context) {
	partitionId, err := strconv.Atoi(c.Param("partition-id"))
	if err != nil {
		log.Printf("[node.handleAddPartition] error while converting partitionId param to int: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	n.replicasMapMutex.Lock()
	if _, ok := n.replicas[partitionId]; ok {
		log.Printf("[node.handleAddPartition] partitionId %v already exists in nodeId %v", partitionId, n.Id)
		c.JSON(http.StatusConflict, gin.H{"error": "this partitionId already exists"})
		return
	}

	n.replicas[partitionId] = replica.NewReplica(n.Id, partitionId, replica.Leader)
	n.replicasMapMutex.Unlock()
	c.JSON(http.StatusOK, nil)
}

func (n *Node) handleSetLeader(c *gin.Context) {
	partitionId, err := strconv.Atoi(c.Param("partition-id"))
	if err != nil {
		log.Printf("[node.handleAddPartition] error while converting partitionId param to int: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	n.replicasMapMutex.Lock()
	if _, ok := n.replicas[partitionId]; !ok {
		n.replicasMapMutex.Unlock()
		log.Printf("[node.handleAddPartition] partitionId %v does not exist in nodeId %v", partitionId, n.Id)
		c.JSON(http.StatusConflict, gin.H{"error": "this partitionId does not exist"})
		return
	}

	n.replicas[partitionId].ConvertToLeader()
	n.replicasMapMutex.Unlock()
	c.JSON(http.StatusOK, nil)
}

func (n *Node) handleSetFollower(c *gin.Context) {
	partitionId, err := strconv.Atoi(c.Param("partition-id"))
	if err != nil {
		log.Printf("[node.handleAddPartition] error while converting partitionId param to int: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	n.replicasMapMutex.Lock()
	if _, ok := n.replicas[partitionId]; !ok {
		n.replicasMapMutex.Unlock()
		log.Printf("[node.handleAddPartition] partitionId %v does not exist in nodeId %v", partitionId, n.Id)
		c.JSON(http.StatusConflict, gin.H{"error": "this partitionId does not exist"})
		return
	}

	n.replicas[partitionId].ConvertToFollower()
	n.replicasMapMutex.Unlock()
	c.JSON(http.StatusOK, nil)
}

func (n *Node) handleDeletePartition(c *gin.Context) {
	partitionId, err := strconv.Atoi(c.Param("partition-id"))
	if err != nil {
		log.Printf("[node.handleDeletePartition] error while converting partitionId param to int: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	n.replicasMapMutex.Lock()
	if _, ok := n.replicas[partitionId]; !ok {
		log.Printf("[node.handleDeletePartition] partitionId %v does not exist in nodeId %v", partitionId, n.Id)
		c.JSON(http.StatusNotFound, gin.H{"error": "this partitionId does not exist"})
		return
	}

	delete(n.replicas, partitionId)
	n.replicasMapMutex.Unlock()
	c.JSON(http.StatusOK, nil)
}

func (n *Node) handleSendPartitionToNode(c *gin.Context) {
	partitionId, err := strconv.Atoi(c.Param("partition-id"))
	if err != nil {
		log.Printf("[node.handleSendPartitionToNode] error while converting partitionId param to int: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	address := c.Param("address")
	if address == "" {
		log.Printf("[node.handleSendPartitionToNode] missing address parameter")
		c.JSON(http.StatusBadRequest, gin.H{"error": "Missing address parameter"})
		return
	}
	log.Printf("[node.handleSendPartitionToNode] sending partition %d to address %s", partitionId, address)

	if err := n.sendSnapshotToNode(partitionId, address); err != nil {
		log.Printf("[node.handleSendPartitionToNode] %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Snapshot sent successfully"})

}

func (n *Node) handleSetRequest(c *gin.Context) {
	partitionId, err := strconv.Atoi(c.Param("partition-id"))
	if err != nil {
		log.Printf("[node.handleSetRequest] error while converting partitionId param to int: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	key := c.Param("key")
	value := c.Param("value")

	err = n.set(partitionId, -1, key, value, replica.Leader)
	if err != nil {
		log.Printf("[node.handleSetRequest] failed to set key '%s' in partition %d: %v", key, partitionId, err)
		c.JSON(http.StatusNotAcceptable, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, nil)
}

func (n *Node) handleGetRequest(c *gin.Context) {
	partitionId, err := strconv.Atoi(c.Param("partition-id"))
	if err != nil {
		log.Printf("[node.handleGetRequest] error while converting partitionId param to int: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	key := c.Param("key")

	value, err := n.get(partitionId, key)
	if err != nil {
		log.Printf("[node.handleGetRequest] failed to get key '%s' from partition %d: %v", key, partitionId, err)
		c.JSON(http.StatusNotAcceptable, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"value": value})
}

func (n *Node) handleDeleteRequest(c *gin.Context) {
	partitionId, err := strconv.Atoi(c.Param("partition-id"))
	if err != nil {
		log.Printf("[node.handleDeleteRequest] error while converting partitionId param to int: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	key := c.Param("key")

	err = n.delete(partitionId, -1, key, replica.Leader)
	if err != nil {
		log.Printf("[node.handleDeleteRequest] failed to delete key '%s' from partition %d: %v", key, partitionId, err)
		c.JSON(http.StatusNotAcceptable, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, nil)
}

func (n *Node) getNodesContainingPartition(partitionId int) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), REQUEST_TIMEOUT)
	defer cancel()

	url := fmt.Sprintf("http://controller:8080/node-metadata/%d", partitionId)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		log.Printf("[node.getNodesContainingPartition] failed to connect to controller: %v", err)
		return nil, err
	}

	resp, err := n.httpClient.Do(req)
	if err != nil {
		log.Printf("[node.getNodesContainingPartition] failed to do http request: %v", err)
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("[node.getNodesContainingPartition] failed to do htpp request: %v", err)
		return nil, fmt.Errorf("failed to fetch metadata: status %d", resp.StatusCode)
	}

	metadata := struct {
		Addresses []string `json:"addresses"`
	}{}
	if err := json.NewDecoder(resp.Body).Decode(&metadata); err != nil {
		log.Printf("[node.getNodesContainingPartition] failed to decode response: %v", err)
		return nil, err
	}

	return metadata.Addresses, nil
}

func (n *Node) sendHeartbeat() error {
	data := url.Values{"NodeID": {strconv.Itoa(n.Id)}}

	resp, err := http.PostForm("http://controller:8080/node-heartbeat", data)
	if err != nil {
		return fmt.Errorf("[node.sendHeartbeat] failed to send heartbeat: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("[node.sendHeartbeat] controller returned non-OK status: %v", resp.Status)
	}

	return nil
}
