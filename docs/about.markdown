---
layout: default
title: About Duva
permalink: /about/
---

## About Duva

Duva is a distributed cache server built for fast and scalable key-value storage. Written in Rust and using the Actor model, it’s designed for high concurrency, fault tolerance, and distributed systems.

### Why Duva?

- **High Concurrency**: Handles thousands of concurrent operations.
- **Fault Tolerance**: Reliable with failure detection and recovery.
- **Scalable**: Works across multiple servers with ease.

### Key Features

- **Core Commands**: SET, GET, KEYS, and SAVE for basic key-value operations.
- **Expiration**: Set time-to-live (TTL) for keys.
- **Persistence**: Save data to RDB or Append-Only Files.
- **Clustering**: Supports replication, failure detection, and node liveness checks.
- **RESP Protocol**: Compatible with Redis-like commands.

### Get Involved

Duva is open-source under the Apache License 2.0. Want to contribute? Visit the [GitHub repository](https://github.com/migorithm/duva) to learn more or submit a pull request!