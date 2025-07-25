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

	n := node.NewNode(id)
	n.Start()
}

func loadConfig() error {
	// TODO
	return nil
}
