---
title: INCRBY
layout: command
description: Increments the integer value stored at key by increment.
syntax: INCRBY key increment
---
If the key doesn't exist, it is initialized to 0 before performing the operation. The value must be parsable as an integer or an error will be returned.

### Example
<div class="command-example">
<pre>
INCRBY key increment
</pre>
</div>


Return value: Integer reply - the value after incrementation

### Notes
- Works only with string values that can be represented as integers
- If the key contains a value of the wrong type or contains a string that can't be represented as integer, an error is returned
- The maximum value of a signed 64-bit integer is 9223372036854775807