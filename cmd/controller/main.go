package main

import (
	"log"

	"github.com/parishadmk/CT_HW4/internal/cluster/controller"
	"github.com/parishadmk/CT_HW4/internal/cluster/controller/docker"
)

func main() {
	dockerClient, err := docker.NewDockerClient()
	if err != nil {
		panic("Failed to create Docker client")
	}
	controller := controller.NewController(dockerClient, 3, 2, "ct_hw3_temp", "ct_hw3-node:latest")
	if err := controller.Start(":8080"); err != nil {
		log.Fatalf("Failed to start controller: %v", err)
	}
}
