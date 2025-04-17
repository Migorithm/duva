---
title: DECR
layout: command
description: Decrease value for the key when it is parsable to integer
syntax: DECR key
---
Decrements the integer value stored at `key` by 1. If the key doesn't exist, it is initialized to 0 before performing the operation. The value must be parsable as an integer or an error will be returned.

### Example
<div class="command-example">
<pre>
duva-cli> SET key "3"
"OK"
duva-cli> DECR key
(integer) 2
duva-cli> SET key "334293482390480948029348230948"
"OK"
duva-cli> DECR mykey
(error) ERR value is not an integer or out of range 
</pre>
</div>


Return value: Integer reply - the value after incrementation

### Notes
- Works only with string values that can be represented as integers
- If the key contains a value of the wrong type or contains a string that can't be represented as integer, an error is returned
- The maximum value of a signed 64-bit integer is 9223372036854775807