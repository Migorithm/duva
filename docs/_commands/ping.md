---
title: PING
layout: command
description: Increase value for the key when it is parsable to integer
syntax: PING
---

- Testing whether a connection is still alive.
- Verifying the server's ability to serve data.
- Measuring latency.

### Example
<div class="command-example">
<pre>
duva-cli> PING
"PONG"
</pre>
</div>


Returns PONG if no argument is provided, otherwise return a copy of the argument as a bulk. This command is useful for:

