---
title: SET
layout: command
description: Store a key-value pair with an optional expiration time.
syntax: SET key value [PX milliseconds]
---

Stores a key-value pair in the cache. You can optionally set a time-to-live (TTL) using the `PX` argument, specified in milliseconds.

### Examples

<div class="command-example">
<pre>
SET mykey "hello" PX 10000
</pre>
</div>

This sets `mykey` to `"hello"` with a 10-second expiration.

### Notes

- If `PX` is not specified, the key does not expire.
- Overwrites the value if the key already exists.