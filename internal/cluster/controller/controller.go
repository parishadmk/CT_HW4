package controller

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/Kafsh-e-Mardane-Varzeshi-Hypo-Test-Team/CT_HW3/internal/cluster/controller/docker"
	"github.com/docker/go-connections/nat"
	"github.com/gin-gonic/gin"
)

type Controller struct {
	dockerClient      *docker.DockerClient
	ginEngine         *gin.Engine
	mu                sync.RWMutex
	partitionCount    int
	replicationFactor int
	nodes             map[int]*NodeMetadata
	partitions        []*PartitionMetadata
	httpClient        *http.Client
	networkName       string
	nodeImage         string
}

func NewController(dockerClient *docker.DockerClient, partitionCount, replicationFactor int, networkName, nodeImage string) *Controller {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())

	c := &Controller{
		dockerClient:      dockerClient,
		ginEngine:         router,
		partitionCount:    partitionCount,
		replicationFactor: replicationFactor,
		nodes:             make(map[int]*NodeMetadata),
		partitions:        make([]*PartitionMetadata, partitionCount),
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:       100,
				IdleConnTimeout:    30 * time.Second,
				DisableCompression: false,
			},
		},
		networkName: networkName,
		nodeImage:   nodeImage,
	}

	for i := range partitionCount {
		c.partitions[i] = &PartitionMetadata{
			PartitionID: i,
			Leader:      -1, // Will be set when first node joins
			Replicas:    make([]int, 0),
		}
	}

	c.setupRoutes()
	// go c.eventHandler()

	return c
}

func (c *Controller) RegisterNode(nodeID int) error {
	c.mu.Lock()
	if _, exists := c.nodes[nodeID]; exists {
		c.mu.Unlock()
		log.Printf("controller::RegisterNode: Node %d already exists.\n", nodeID)
		return errors.New("node already exists")
	}
	c.nodes[nodeID] = &NodeMetadata{
		ID:     nodeID,
		Status: Creating,
	}
	c.mu.Unlock()

	// Create a new docker container for the node
	imageName := c.nodeImage
	networkName := c.networkName
	tcpPort := "9000"
	httpPort := "8000"

	err := c.dockerClient.CreateNodeContainer(
		imageName,
		nodeID,
		networkName,
		nat.Port(tcpPort),
		nat.Port(httpPort),
	)
	if err != nil {
		log.Printf("controller::RegisterNode: Failed to create docker container for node %d\n", nodeID)
		return errors.New("failed to create container")
	}

	c.mu.Lock()
	node := c.nodes[nodeID]
	node.TcpAddress = fmt.Sprintf("node-%d:%s", nodeID, tcpPort)
	node.HttpAddress = fmt.Sprintf("http://node-%d:%s", nodeID, httpPort)
	node.Status = Syncing
	c.mu.Unlock()

	go c.makeNodeReady(nodeID)

	return nil
}

func (c *Controller) removeNode(nodeID int) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	_, exists := c.nodes[nodeID]
	if !exists {
		return errors.New("node not found")
	}

	delete(c.nodes, nodeID)

	// Stop and remove the docker container
	err := c.dockerClient.RemoveNodeContainer(nodeID)
	if err != nil {
		log.Printf("controller::removeNode: Failed to remove docker container for node %d: %v\n", nodeID, err)
		return err
	}

	log.Printf("controller::removeNode: Node %d removed successfully\n", nodeID)

	return nil
}

func (c *Controller) Start(addr string) error {
	if err := c.RegisterNode(1); err != nil {
		log.Fatalf("Failed to create initial node: %v", err)
	}

	go c.monitorHeartbeat()

	err := c.Run(addr)
	if err != nil {
		log.Printf("controller::Start: Failed to run http server")
		return err
	}

	return nil
}

func (c *Controller) makeNodeReady(nodeID int) {
	partitionsToAssign := make([]int, 0)
	c.mu.Lock()
	for _, partition := range c.partitions {
		if len(partition.Replicas) < c.replicationFactor {
			partitionsToAssign = append(partitionsToAssign, partition.PartitionID)
		}
	}
	c.mu.Unlock()

	c.replicatePartitions(partitionsToAssign, nodeID)

	c.mu.Lock()
	c.nodes[nodeID].Status = Alive
	c.mu.Unlock()
	log.Printf("controller::makeNodeReady: Node %d is now ready\n", nodeID)
}

func (c *Controller) replicatePartitions(partitionsToAssign []int, nodeID int) {
	for _, partitionID := range partitionsToAssign {
		partition := c.partitions[partitionID]
		for i := 0; i < 3; i++ {
			err := c.replicate(partitionID, nodeID)
			if err == nil {
				c.mu.Lock()
				if partition.Leader == -1 {
					partition.Leader = nodeID
				} else {
					partition.Replicas = append(partition.Replicas, nodeID)
				}
				c.nodes[nodeID].partitions = append(c.nodes[nodeID].partitions, partitionID)
				c.mu.Unlock()
				log.Printf("controller::makeNodeReady: replicate successfully partition %d to node %d\n", partitionID, nodeID)
				break
			} else {
				log.Printf("controller::makeNodeReady: Failed to replicate %d in node %d: %v\n", partitionID, nodeID, err)
			}
			time.Sleep(1 * time.Second)
		}
	}
}

func (c *Controller) replicate(partitionID, nodeID int) error {
	var addr string
	c.mu.RLock()
	if c.partitions[partitionID].Leader == -1 {
		addr = fmt.Sprintf("%s/add-partition/%d", c.nodes[nodeID].HttpAddress, partitionID)
	} else {
		addr = fmt.Sprintf("%s/send-partition/%d/%s", c.nodes[c.partitions[partitionID].Leader].HttpAddress, partitionID, c.nodes[nodeID].TcpAddress)
	}
	c.mu.RUnlock()

	resp, err := c.doNodeRequest("POST", addr)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return errors.New("resp status not OK: " + resp.Status)
	}

	return nil
}

func (c *Controller) doNodeRequest(method, addr string) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, method, addr, nil)
	if err != nil {
		return nil, err
	}

	return c.httpClient.Do(req)
}

func (c *Controller) monitorHeartbeat() {
	for {
		c.mu.Lock()
		for _, node := range c.nodes {
			if time.Since(node.lastSeen) > 5*time.Second {
				if node.Status == Alive {
					log.Printf("controller::monitorHeartbeat: Node %d is not responding\n", node.ID)
					node.Status = Dead

					go c.handleFailover(node.ID)
				}
			}
		}
		c.mu.Unlock()

		time.Sleep(2 * time.Second)
	}
}

func (c *Controller) handleFailover(nodeID int) {
	log.Printf("controller::handleFailover: Handling failover for node %d\n", nodeID)

	c.mu.Lock()
	defer c.mu.Unlock()
	node, exists := c.nodes[nodeID]
	if !exists || node.Status != Dead {
		return
	}

	for _, partitionID := range node.partitions {
		if c.partitions[partitionID].Leader == nodeID {
			// set another node as leader
			if len(c.partitions[partitionID].Replicas) > 0 {
				newLeader := c.partitions[partitionID].Replicas[0]
				c.partitions[partitionID].Leader = newLeader
				log.Printf("controller::handleFailover: Node %d is now the leader for partition %d\n", newLeader, partitionID)
				// notify the new leader to take over
				addr := fmt.Sprintf("%s/set-leader/%d", c.nodes[newLeader].HttpAddress, partitionID)
				resp, err := c.doNodeRequest("POST", addr)
				if err != nil {
					log.Printf("controller::handleFailover: Failed to notify new leader for partition %d: %v\n", partitionID, err)
					continue
				}
				defer resp.Body.Close()
				if resp.StatusCode != http.StatusOK {
					log.Printf("controller::handleFailover: Failed to set new leader for partition %d: %s\n", partitionID, resp.Status)
				} else {
					log.Printf("controller::handleFailover: New leader for partition %d is node %d\n", partitionID, newLeader)
					// remove replica[0]
					c.partitions[partitionID].Replicas = c.partitions[partitionID].Replicas[1:]
				}
			}
		} else {
			// remove this node from replicas
			for i, replica := range c.partitions[partitionID].Replicas {
				if replica == nodeID {
					c.partitions[partitionID].Replicas = append(c.partitions[partitionID].Replicas[:i], c.partitions[partitionID].Replicas[i+1:]...)
					break
				}
			}
		}
	}
}

func (c *Controller) reviveNode(nodeID int) {
	c.mu.Lock()
	node, exists := c.nodes[nodeID]
	if !exists || node.Status != Dead {
		c.mu.Unlock()
		return
	}
	node.Status = Syncing
	c.mu.Unlock()

	log.Printf("controller::reviveNode: Reviving node %d\n", nodeID)

	partitionsToReplicate := make([]int, 0)
	c.mu.Lock()
	for _, partition := range node.partitions {
		partitionsToReplicate = append(partitionsToReplicate, partition)
	}
	node.partitions = make([]int, 0)
	c.mu.Unlock()
	c.replicatePartitions(partitionsToReplicate, nodeID)

	c.mu.Lock()
	c.nodes[nodeID].Status = Alive
	c.mu.Unlock()
	log.Printf("controller::reviveNode: Node %d revived successfully\n", nodeID)
}

func (c *Controller) changeLeader(partitionID, newLeaderID int) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	partition := c.partitions[partitionID]

	// notify the old leader to become follower
	oldLeaderID := partition.Leader
	if oldLeaderID != -1 {
		addr := fmt.Sprintf("%s/set-follower/%d", c.nodes[oldLeaderID].HttpAddress, partitionID)
		resp, err := c.doNodeRequest("POST", addr)
		if err != nil {
			log.Printf("controller::changeLeader: Failed to notify old leader %d for partition %d: %v\n", oldLeaderID, partitionID, err)
			return fmt.Errorf("failed to notify old leader")
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			log.Printf("controller::changeLeader: Failed to set old leader %d as follower for partition %d: %s\n", oldLeaderID, partitionID, resp.Status)
			return fmt.Errorf("failed to set old leader as follower for partition %d: %v", partitionID, resp.Status)
		}
		log.Printf("controller::changeLeader: Node %d is now a follower for partition %d\n", oldLeaderID, partitionID)
	}

	// notify the new leader to take over
	addr := fmt.Sprintf("%s/set-leader/%d", c.nodes[newLeaderID].HttpAddress, partitionID)
	resp, err := c.doNodeRequest("POST", addr)
	if err != nil {
		log.Printf("controller::changeLeader: Failed to notify new leader for partition %d: %v\n", partitionID, err)
		return fmt.Errorf("failed to notify new leader")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		log.Printf("controller::changeLeader: Failed to set new leader for partition %d: %s\n", partitionID, resp.Status)
		return fmt.Errorf("failed to set new leader for partition %d: %v", partitionID, resp.Status)
	}

	// remove new leader from replicas
	for i, replica := range partition.Replicas {
		if replica == newLeaderID {
			partition.Replicas = append(partition.Replicas[:i], partition.Replicas[i+1:]...)
			log.Printf("controller::changeLeader: Removed node %d from replicas of partition %d\n", newLeaderID, partitionID)
			break
		}
	}

	// add old leader to replicas
	if oldLeaderID != -1 && oldLeaderID != newLeaderID {
		partition.Replicas = append(partition.Replicas, oldLeaderID)
		log.Printf("controller::changeLeader: Added old leader %d to replicas of partition %d\n", oldLeaderID, partitionID)
	}

	partition.Leader = newLeaderID
	log.Printf("controller::changeLeader: Partition %d leader changed to node %d\n", partitionID, newLeaderID)

	return nil
}

func (c *Controller) removePartitionReplica(partitionID, nodeID int) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	partition := c.partitions[partitionID]
	if partition == nil {
		return fmt.Errorf("partition %d not found", partitionID)
	}

	for i, replica := range partition.Replicas {
		if replica == nodeID {
			partition.Replicas = append(partition.Replicas[:i], partition.Replicas[i+1:]...)
			log.Printf("controller::removePartitionReplica: Removed node %d from replicas of partition %d\n", nodeID, partitionID)
			return nil
		}
	}

	addr := fmt.Sprintf("%s/delete-partition/%d", c.nodes[nodeID].HttpAddress, partitionID)

	resp, err := c.doNodeRequest("POST", addr)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return errors.New("resp status not OK: " + resp.Status)
	}

	log.Printf("controller::removePartitionReplica: Node %d removed from partition %d successfully\n", nodeID, partitionID)
	return nil
}
