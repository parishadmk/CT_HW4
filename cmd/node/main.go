package main

import (
	"log"
	"os"
	"strconv"

	"github.com/parishadmk/CT_HW4/internal/cluster/node"
)

func main() {
	err := loadConfig()
	if err != nil {
		log.Fatalf("[node.Start] can not load config due to: %v", err)
	}

	id, err := strconv.Atoi(os.Getenv("NODE-ID"))
	if err != nil {
		log.Fatalf("[main (node)] failed to get 'NODE-ID' env variable: %v", err)
	}

	// Read the new environment variable
	controllerAddr := os.Getenv("CONTROLLER_ADDR")
	if controllerAddr == "" {
		log.Fatalf("[main (node)] CONTROLLER_ADDR env variable not set")
	}

	// This is the corrected line that passes BOTH arguments
	n := node.NewNode(id, controllerAddr)
	n.Start()
}

func loadConfig() error {
	// TODO
	return nil
}
