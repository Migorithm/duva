---
layout: default
title: Command Reference
permalink: /commands/
---

# Command Reference

Below is a list of supported commands in Duva, their syntax, and what they do.

## Core Commands

### `SET`
Store a key-value pair in the cache.

**Syntax:**

SET <key> <value> [PX <ttl-in-mills>]


**Example:**
SET mykey "Hello World" PX 60


Stores the value `"Hello World"` with a TTL of 60 mills.

---

### `GET`
Retrieve the value associated with a key.

**Syntax:**

GET <key>

**Example:**

---

### `KEYS`
Retrieve keys matching a pattern.

**Syntax:**