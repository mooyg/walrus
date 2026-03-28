# Walrus 🦭

A tiny append-only commit log written in Go. Built as a minimal queue system for a personal face indexing pipeline.

Inspired by Kafka, but tiny and purpose-built - no cluster, no partitions, just durable ordered writes and offset-based reads.

## Install

### Binary

Download a prebuilt binary from [GitHub Releases](https://github.com/mooyg/walrus/releases), or install with Go:

```bash
go install github.com/mooyg/walrus/cmd/walrus@latest
```

### As a library

```bash
go get github.com/mooyg/walrus
```

## Usage

### Running the server

```bash
walrus -port 9092 -data-dir ./data -log-level info
```

| Flag | Default | Description |
|------|---------|-------------|
| `-port` | `9092` | gRPC listen port |
| `-data-dir` | `./data` | Base directory for commit log storage |
| `-log-level` | `info` | Log level (debug, info, warn, error) |
| `-version` | | Print version and exit |

### Embedding the server in your application

```go
package main

import "github.com/mooyg/walrus"

func main() {
	srv, err := walrus.NewServer(walrus.Config{
		Port:    9092,
		DataDir: "/tmp/walrus",
	})
	if err != nil {
		panic(err)
	}

	defer srv.Stop()
	srv.Start()
}
```

### Using the gRPC client

```go
package main

import (
	"context"

	proto "github.com/mooyg/walrus/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	conn, err := grpc.NewClient("localhost:9092",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	client := proto.NewBrokerServiceClient(conn)

	// Produce a message
	resp, _ := client.Produce(context.Background(), &proto.ProduceRequest{
		Topic: "events",
		Data:  []byte("hello walrus"),
	})

	// Fetch messages
	fetch, _ := client.Fetch(context.Background(), &proto.FetchRequest{
		Topic:  "events",
		Offset: resp.Offset,
		Limit:  10,
	})

	// Commit consumer offset
	client.CommitOffset(context.Background(), &proto.CommitOffsetRequest{
		ConsumerId: "my-consumer",
		Topic:      "events",
		Offset:     fetch.LastOffset,
	})
}
```

## How it works

Messages are appended to a binary file. Each entry is a 4-byte length header followed by the raw payload. An in-memory offset index is rebuilt from the file on startup, so it survives restarts. Reads and writes go through a single goroutine event loop via channels.

## What's done

- Append-only file log with fsync on every write
- Offset-based batch reads (`ReadFrom(offset, max)`)
- Index rebuild on open (crash recovery)
- Concurrent read/write via channel event loop
- Broker layer managing named queues
- gRPC API (Produce, Fetch, CommitOffset)
- Embeddable server library
- Automated releases via GoReleaser

## What's next

- [ ] **Metrics** - write/read counts, latency, bytes written. Prometheus or a simple HTTP endpoint.
- [ ] **Perf work** - benchmark first, then look at batched writes and buffered I/O.
