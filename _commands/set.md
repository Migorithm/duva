---
title: SET
description: Store a key-value pair in the cache.
category: Core Commands
syntax: SET key value [EX seconds]
arguments:
  - name: key
    type: string
    description: The key to set.
  - name: value
    type: string
    description: The value to associate with the key.
  - name: EX seconds
    type: integer
    description: Optional. Set the expiration time in seconds (TTL).
examples:
  - command: SET mykey "hello"
    description: Sets the key `mykey` to value `"hello"`.
  - command: SET mykey "hello" EX 60
    description: Sets the key `mykey` to value `"hello"` with a 60-second TTL.
---

Stores a key-value pair in Duva's cache. Optionally, you can set a time-to-live (TTL) for the key, after which it will automatically expire.