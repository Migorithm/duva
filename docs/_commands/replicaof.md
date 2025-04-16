---
title: REPLICAOF
layout: command
description: Change the replication settings of a node on the fly.
syntax: REPLICAOF <host port>
---

If a server is already a replica of some master, REPLICAOF hostname port will stop the replication against the old server and start the synchronization against the new one, discarding the old dataset.

### Example
<div class="command-example">
<pre>
duva-cli> REPLICAOF 127.0.0.1 6799
"OK"
</pre>
</div>


