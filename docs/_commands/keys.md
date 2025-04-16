---
title: KEYS
layout: command
description: Retrieve keys matching a specific pattern.
syntax: KEYS pattern
---
Returns a list of keys that match the given pattern. Supports simple glob-style patterns.

### Example
<div class="command-example">
<pre>
KEYS my*
</pre>
</div>


Returns all keys starting with `my`, such as `mykey`, `mykey2`.

### Notes
- Use `*` to match any characters, `?` for a single character.
- Be cautious with large datasets, as this can be resource-intensive.