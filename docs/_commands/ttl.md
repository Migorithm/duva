---
title: TTL
layout: command
description: Returns the remaining time to live of a key that has a timeout.
syntax: TTL key
---
This introspection capability allows a client to check how many seconds a given key will continue to be part of the dataset.

### Example
<div class="command-example">
<pre>
duva-cli> SET key "Hello" px 10000
"OK"
duva-cli> TTL key
(integer) 9 
</pre>
</div>




### Notes
- When key doesn't exist: **(integer) -1**
- Reply should be given in seconds (not millisecond)