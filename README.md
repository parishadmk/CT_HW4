-----

# CT\_HW4

Computer Technology Course - Homework 4

-----

# Distributed In-Memory Database

A distributed in-memory key-value store written in Go. The system follows a single-leader architecture and is designed to be scalable, highly available, and easy to deploy using Docker.

## Features

  * In-memory key-value storage with a distributed architecture.
  * Single-leader replication model for data consistency.
  * **High-Availability Control Plane:** Uses **`etcd`** to manage all cluster state, eliminating the controller as a single point of failure.
  * **Redundant Controllers:** Supports multiple, redundant controller instances for a resilient control plane.
  * **Automatic Failover:** The system automatically detects data node failures and promotes a replica to be the new leader.
  * Key partitioning is determined by `Hash(key) % number_of_partitions`.
  * Leader replicas use LSM trees to persist changes and initialize new replicas.
  * A load balancer uses metadata from `etcd` to route client requests to the appropriate nodes.
  * Clients can send key-value requests and manage the cluster by dynamically adding nodes.
  * Fully containerized for easy deployment with Docker.

## Project Structure

  * **`etcd`**: A distributed key-value store that reliably stores all cluster state, including node metadata and partition leadership information. This allows the control plane to be stateless and resilient.
  * **Controller**: Manages node lifecycles and partition assignments. Multiple controller instances can run concurrently, reading and writing shared state to `etcd` for high availability.
  * **Nodes (Replicas)**: Store and manage data for specific partitions. Leaders persist logs using LSM trees and replicate changes to followers.
  * **Load Balancer**: Forwards client requests to the appropriate leader nodes by querying cluster metadata from `etcd`.
  * **Client**: A command-line interface to send key-value operations and manage the cluster.

## Running with Docker

This project uses a two-step process to ensure all images are built before the services are started.

**1. Build All Service Images:**
Run this command once in the project's root to build the images for all services, including the `node` and `client`.

```bash
docker compose --profile "*" build
```

**2. Start the Core Services:**
This command starts the controllers, `etcd`, and the load balancer.

```bash
docker-compose up
```

**3. Interact with the Client:**
Once the system is running, you can use the client to interact with the database.

```bash
# Set a value
docker-compose run --entrypoint /app/client client set --key=hello --value=world

# Get a value
docker-compose run --entrypoint /app/client client get --key=hello

# Add a new node to the cluster
docker-compose run --entrypoint /app/client client add-node --id=2

# Kill a single controller
docker kill controller
```

## Contributors

  * Parishad Mokhber
