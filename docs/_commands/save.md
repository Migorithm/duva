---
title: SAVE
layout: command
description: Dump data to a designated file path.
syntax: SAVE
---
Persists the current cache data to a file (e.g., an RDB file) for durability.


Dumps the current data to the configured file path.

### Notes
- File path is set via configuration 
<div class="command-example">
<pre>
cargo run -- --dir directory-path --dbfilename filename
</pre>
</div>
- Overwrites the existing file if it exists.