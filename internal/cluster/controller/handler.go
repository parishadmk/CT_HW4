package controller

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
)

func (c *Controller) setupRoutes() {
	c.ginEngine.GET("/metadata", c.handleGetMetadata)
	c.ginEngine.GET("/node-metadata/:partitionID", c.handleGetNodeMetadata)

	c.ginEngine.POST("/node-heartbeat", c.handleHeartbeat)

	c.ginEngine.POST("/node/add", c.handleRegisterNode)
	c.ginEngine.POST("/node/remove", c.handleRemoveNode)

	c.ginEngine.POST("/partition/move-replica", c.handleMoveReplica)
	c.ginEngine.POST("/partition/set-leader", c.handleSetLeader)
}

func (c *Controller) Run(addr string) error {
	return c.ginEngine.Run(addr)
}

func (c *Controller) handleGetMetadata(ctx *gin.Context) {
	ctx2, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	nodes, err := c.etcdStore.ListJSON(ctx2, "/nodes/", func() interface{} { return &NodeMetadata{} })
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	parts, err := c.etcdStore.ListJSON(ctx2, "/partitions/", func() interface{} { return &PartitionMetadata{} })
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	metadata := struct {
		NodeAddresses map[int]string       `json:"nodes"`
		Partitions    []*PartitionMetadata `json:"partitions"`
	}{NodeAddresses: make(map[int]string), Partitions: make([]*PartitionMetadata, len(parts))}

	for _, nraw := range nodes {
		nm := nraw.(*NodeMetadata)
		metadata.NodeAddresses[nm.ID] = nm.HttpAddress
	}
	for i, praw := range parts {
		metadata.Partitions[i] = praw.(*PartitionMetadata)
	}

	ctx.JSON(http.StatusOK, metadata)
}

func (c *Controller) handleGetNodeMetadata(ctx *gin.Context) {
	partitionID, err := strconv.Atoi(ctx.Param("partitionID"))
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Invalid partition ID"})
		return
	}

	ctx2, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var part PartitionMetadata
	if err := c.etcdStore.GetJSON(ctx2, fmt.Sprintf("/partitions/%d", partitionID), &part); err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	addresses := make([]string, 0, len(part.Replicas))
	for _, id := range part.Replicas {
		var nm NodeMetadata
		if err := c.etcdStore.GetJSON(ctx2, fmt.Sprintf("/nodes/%d", id), &nm); err == nil {
			addresses = append(addresses, nm.TcpAddress)
		}
	}
	ctx.JSON(http.StatusOK, gin.H{"addresses": addresses})
}

func (c *Controller) handleHeartbeat(ctx *gin.Context) {
	nodeID, err := strconv.Atoi(ctx.PostForm("NodeID"))
	if err != nil {
		return
	}

	c.mu.Lock()
	node, ok := c.nodes[nodeID]
	c.mu.Unlock()
	if !ok {
		ctx2, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		var nm NodeMetadata
		if err := c.etcdStore.GetJSON(ctx2, fmt.Sprintf("/nodes/%d", nodeID), &nm); err == nil {
			c.mu.Lock()
			c.nodes[nodeID] = &nm
			node = &nm
			c.mu.Unlock()
		} else {
			cancel()
			ctx.Status(http.StatusNotFound)
			return
		}
		cancel()
	}

	c.mu.Lock()
	if node.Status == Dead {
		log.Printf("controller::handleHeartbeat: Node %d revived\n", nodeID)
		go c.reviveNode(nodeID)
	}
	node.lastSeen = time.Now()
	c.mu.Unlock()
	ctx.Status(http.StatusOK)
}

func (c *Controller) handleRegisterNode(ctx *gin.Context) {
	nodeID, err := strconv.Atoi(ctx.PostForm("NodeID"))
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Invalid node ID"})
		return
	}

	err = c.RegisterNode(nodeID)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to register node"})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{"message": "Node is creating"})
}

func (c *Controller) handleRemoveNode(ctx *gin.Context) {
	nodeID, err := strconv.Atoi(ctx.PostForm("NodeID"))
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Invalid node ID"})
		return
	}

	c.mu.Lock()
	if _, exists := c.nodes[nodeID]; !exists {
		c.mu.Unlock()
		ctx.JSON(http.StatusNotFound, gin.H{"error": "Node not found"})
		return
	}
	c.nodes[nodeID].Status = Dead
	c.mu.Unlock()

	c.handleFailover(nodeID)

	err = c.removeNode(nodeID)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to remove node"})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{"message": "Node removed successfully"})
	log.Printf("controller::handleRemoveNode: Node %d removed\n", nodeID)
}

func (c *Controller) handleSetLeader(ctx *gin.Context) {
	var req struct {
		PartitionID int `json:"partition_id"`
		NodeID      int `json:"node_id"`
	}
	if err := ctx.ShouldBindJSON(&req); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	c.mu.Lock()
	if req.PartitionID < 0 || req.PartitionID >= len(c.partitions) {
		c.mu.Unlock()
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Invalid partition ID"})
		return
	}

	if c.partitions[req.PartitionID].Leader == req.NodeID {
		c.mu.Unlock()
		ctx.JSON(http.StatusOK, gin.H{"message": "Node is already the leader"})
		return
	}

	// Check if the new leader is already a replica, if not error
	partition := c.partitions[req.PartitionID]
	exists := false
	for _, replica := range partition.Replicas {
		if replica == req.NodeID {
			exists = true
			break
		}
	}
	if !exists {
		c.mu.Unlock()
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Node is not a replica of the partition"})
		return
	}
	c.mu.Unlock()

	c.changeLeader(req.PartitionID, req.NodeID)

	ctx.JSON(http.StatusOK, gin.H{"message": "Leader set successfully"})
	log.Printf("controller::handleSetLeader: Node %d is now the leader for partition %d\n", req.NodeID, req.PartitionID)
}

func (c *Controller) handleRebalance(ctx *gin.Context) {

}

func (c *Controller) handleMoveReplica(ctx *gin.Context) {
	var req struct {
		PartitionID int `json:"partition_id"`
		From        int `json:"from"`
		To          int `json:"to"`
	}
	if err := ctx.ShouldBindJSON(&req); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	c.mu.RLock()
	if req.PartitionID < 0 || req.PartitionID >= len(c.partitions) {
		c.mu.RUnlock()
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Invalid partition ID"})
		return
	}

	partition := c.partitions[req.PartitionID]

	isLeader := false
	if partition.Leader == req.From {
		isLeader = true
	} else {
		exists := false
		for _, replica := range partition.Replicas {
			if replica == req.From {
				exists = true
				break
			}
		}
		if !exists {
			c.mu.RUnlock()
			ctx.JSON(http.StatusBadRequest, gin.H{"error": "Replica not found in partition"})
			return
		}
	}
	c.mu.RUnlock()

	log.Printf("controller::handleMoveReplica: Moving replica from node %d to node %d for partition %d\n", req.From, req.To, req.PartitionID)
	c.replicate(req.PartitionID, req.To)
	if isLeader {
		c.changeLeader(req.PartitionID, req.To)
		log.Printf("controller::handleMoveReplica: Node %d is now the leader for partition %d after moving replica\n", req.To, req.PartitionID)
	}
	c.removePartitionReplica(req.PartitionID, req.From)

	log.Printf("controller::handleMoveReplica: Replica moved from node %d to node %d for partition %d\n", req.From, req.To, req.PartitionID)
	ctx.JSON(http.StatusOK, gin.H{"message": "Replica moved successfully"})
}

func (c *Controller) handleReadyCheck(ctx *gin.Context) {

}
