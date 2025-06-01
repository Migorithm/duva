---
title: DECRBY
layout: command
description: The DECRBY command reduces the value stored at the specified key by the specified decrement
syntax: DECRBY key increment
---
If the key does not exist, it is initialized with a value of 0 before performing the operation. If the key's value is not of the correct type or cannot be represented as an integer, an error is returned. This operation is limited to 64-bit signed integers.

### Example
<div class="command-example">
<pre>
duva-cli> SET mykey 10
"OK"
duva-cli> DECRBY mykey 3
(integer) 7
</pre>
</div>


Return value: Integer reply - the value after incrementation

### Notes
- Works only with string values that can be represented as integers
- If the key contains a value of the wrong type or contains a string that can't be represented as integer, an error is returned
- The maximum value of a signed 64-bit integer is 9223372036854775807