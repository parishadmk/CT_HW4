package docker

import (
	"context"
	"errors"
	"log"
	"strconv"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
)

type DockerClient struct {
	cli     *client.Client
	respIDs map[string]string
}

func NewDockerClient() (*DockerClient, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		log.Printf("docker::NewDockerClient: Failed to create docker client\n")
		return nil, errors.New("failed to create docker client")
	}

	return &DockerClient{cli: cli, respIDs: make(map[string]string)}, nil
}

func (d *DockerClient) CreateNodeContainer(imageName string, nodeID int, networkName string, tcpPort nat.Port, httpPort nat.Port) error {
	ctx := context.Background()
	nodeName := "node-" + strconv.Itoa(nodeID)

	resp, err := d.cli.ContainerCreate(ctx, &container.Config{
		Image: imageName,
		ExposedPorts: nat.PortSet{
			tcpPort:  {},
			httpPort: {},
		},
		Env: []string{
			"NODE-ID=" + strconv.Itoa(nodeID),
		},
	}, &container.HostConfig{
		NetworkMode: container.NetworkMode(networkName),
	}, nil, nil, nodeName)
	if err != nil {
		log.Printf("docker::CreateNodeContainer: Failed to create container %s: %v\n", nodeName, err)
		return errors.New("failed to create node container")
	}
	log.Printf("docker::CreateNodeContainer: %s created successfully\n", nodeName)

	if err := d.cli.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		log.Printf("docker::CreateNodeContainer: Failed to start container %s\n", nodeName)
		return errors.New("failed to start node container")
	}
	log.Printf("docker::CreateNodeContainer: %s started successfully\n", nodeName)

	time.Sleep(5 * time.Second)

	d.respIDs[nodeName] = resp.ID
	return nil
}

func (d *DockerClient) RemoveNodeContainer(nodeID int) error {
	ctx := context.Background()
	nodeName := "node-" + strconv.Itoa(nodeID)

	if _, exists := d.respIDs[nodeName]; !exists {
		log.Printf("docker::RemoveNodeContainer: Container %s does not exist\n", nodeName)
		return errors.New("container does not exist")
	}

	if err := d.cli.ContainerStop(ctx, d.respIDs[nodeName], container.StopOptions{}); err != nil {
		log.Printf("docker::RemoveNodeContainer: Failed to stop container %s\n", nodeName)
		return errors.New("failed to stop node container")
	}
	log.Printf("docker::RemoveNodeContainer: %s stopped successfully\n", nodeName)

	if err := d.cli.ContainerRemove(ctx, d.respIDs[nodeName], container.RemoveOptions{Force: true}); err != nil {
		log.Printf("docker::RemoveNodeContainer: Failed to remove container %s\n", nodeName)
		return errors.New("failed to remove node container")
	}
	log.Printf("docker::RemoveNodeContainer: %s removed successfully\n", nodeName)

	delete(d.respIDs, nodeName)
	return nil
}
