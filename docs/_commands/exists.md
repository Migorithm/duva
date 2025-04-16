---
title: EXISTS
layout: command
description: Check if given keys exist
syntax: EXISTS key [key ...]
---
Returns if key exists.


### Example
<div class="command-example">
<pre>
duva-cli> SET key1 "Hello"
"OK"
duva-cli> EXISTS key1
(integer) 1
duva-cli> EXISTS nosuchkey
(integer) 0
duva-cli> SET key2 "World"
"OK"
duva-cli> EXISTS key1 key2 nosuchkey
(integer) 2
duva-cli> 
</pre>
</div>


Return value: Integer reply - the value after incrementation

### Notes
- The user should be aware that if the same existing key is mentioned in the arguments multiple times, it will be counted multiple times. So if somekey exists, EXISTS somekey somekey will return 2.
