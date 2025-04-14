---
title: SAVE
layout: command
description: Dump data to a designated file path.
syntax: SAVE
---
Persists the current cache data to a file (e.g., an RDB file) for durability.

### Example
```sh
SAVE
```
Dumps the current data to the configured file path.

### Notes
- File path is set via configuration (see `cargo run -- --dir directory-path --dbfilename filename`).
- Overwrites the existing file if it exists.