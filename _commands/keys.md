---
title: KEYS
description: Retrieve keys matching a specific pattern.
category: Core Commands
syntax: KEYS pattern
arguments:
  - name: pattern
    type: string
    description: The pattern to match keys against (supports glob-style patterns).
examples:
  - command: KEYS user*
    description: Returns all keys starting with `user`.
---

Finds all keys in the cache that match the given pattern. Use sparingly in production as it may impact performance with large datasets.