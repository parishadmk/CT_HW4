package main

import (
	"log"

	"github.com/Kafsh-e-Mardane-Varzeshi-Hypo-Test-Team/CT_HW3/internal/cluster/loadbalancer"
)

func main() {
	lb := loadbalancer.NewLoadBalancer("http://controller:8080")

	if err := lb.Run(":9001"); err != nil {
		log.Fatalf("Failed to start load balancer: %v", err)
	}
}
