package main

import (
	"log"

	"github.com/parishadmk/CT_HW4/internal/cluster/loadbalancer"
)

func main() {
	lb := loadbalancer.NewLoadBalancer("http://controller:8080")

	if err := lb.Run(":9001"); err != nil {
		log.Fatalf("Failed to start load balancer: %v", err)
	}
}
