# Walrus 🦭

A tiny append-only commit log written in Go. Built as a minimal queue system for a personal face indexing pipeline.

Inspired by Kafka, but tiny and purpose-built - no cluster, no partitions, just durable ordered writes and offset-based reads.

## How it works

Messages are appended to a binary file. Each entry is a 4-byte length header followed by the raw payload. An in-memory offset index is rebuilt from the file on startup, so it survives restarts. Reads and writes go through a single goroutine event loop via channels.

## What's done

- Append-only file log with fsync on every write
- Offset-based batch reads (`ReadFrom(offset, max)`)
- Index rebuild on open (crash recovery)
- Concurrent read/write via channel event loop

## What's next

- [ ] **Broker layer** - sit on top of the commit log, manage named queues, handle lifecycle.
- [ ] **gRPC API** - Produce and Consume RPCs for external consumers (like the face indexing pipeline).
- [ ] **Metrics** - write/read counts, latency, bytes written. Prometheus or a simple HTTP endpoint.
- [ ] **Perf work** - benchmark first, then look at batched writes and buffered I/O. Blocked on broker.

## Status

This is a work in progress. I work on it in my free time alongside a full-time job, so progress is slow but steady.
