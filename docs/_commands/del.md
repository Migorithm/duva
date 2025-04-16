---
title: DEL
layout: command
description: Delete keys
syntax: DEL key [key ...]
---
Removes the specified keys. A key is ignored if it does not exist.


### Example
<div class="command-example">
<pre>
duva-cli> SET key1 "Hello"
"OK"
duva-cli> SET key2 "World"
"OK"
duva-cli> DEL key1 key2 key3
(integer) 2
</pre>
</div>


Return value: (integer) the number of keys that were removed.


