---
title: GET
layout: command
description: Retrieve the value associated with a key.
syntax: GET key
---
Retrieves the value stored at the specified key. Returns `nil` if the key does not exist or has expired.

### Example
```sh
GET mykey
```
If `mykey` was set to `"hello"`, this returns `"hello"`.

### Notes
- Case-sensitive key matching.