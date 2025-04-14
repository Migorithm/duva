---
title: GET
description: Retrieve the value associated with a key.
category: Core Commands
syntax: GET key
arguments:
  - name: key
    type: string
    description: The key to retrieve.
examples:
  - command: GET mykey
    description: Retrieves the value of `mykey`.
---

Fetches the value stored at the specified key. Returns `nil` if the key does not exist or has expired.