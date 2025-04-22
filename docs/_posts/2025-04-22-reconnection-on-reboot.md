---
layout: post
title: "Reconnection on reboot"
date: 2025-04-22 00:00:01 +0900
categories: posts
---

<div class="post-content">

  <h2>Duva reconnects to peers automatically after reboot — and it feels <em>really</em> good</h2>

  <p>
    One of the little things we added to <strong>Duva</strong>, Rust-powered distributed key-value store, is that when a node reboots, it tries to reconnect to the same peers it was talking to before.
  </p>

  <p>
    It just works.<br>
    No re-bootstrap, no manual config tweaking, no weird “am I part of the cluster?” delays.
  </p>

  <p>
    The node loads a small file with known peers and tries to reconnect as soon as it boots. If the file's older than some amount of time(should be configurable), it just skips it — no point trying to gossip with ghosts.
  </p>

  <p>
    It sounds simple, but I didn't realize how <em>nice</em> it would feel until I started rebooting nodes in dev/testing and seeing them instantly slide back into the cluster like nothing happened.
  </p>

  <h3>But it wasn’t completely painless</h3>

  <p>
    I ran into some interesting edge cases during this:
  </p>

  <ul>
    <li><strong>How does a failed node know if it was a leader?</strong>  
      If the node crashed while it was the leader, and it loads back up with a stale view, it might falsely assume it still leads the cluster. I had to make sure the node always re-validates its role by trying to talk to others before assuming leadership.
    </li>

    <li><strong>Avoiding conflict with the <code>replicaof</code> command:</strong>  
      Duva has a <code>replicaof</code> command that lets you point a node to follow a specific leader manually. But that creates tension: should the node obey <code>replicaof</code>, or reconnect to old peers from before the crash? I had to make sure the reboot reconnection logic respects <code>replicaof</code> if it’s set — basically, user intent wins over automation.
    </li>
  </ul>

  <p>
    These were fun challenges to debug, and we're pretty happy with how the final setup behaves. It keeps things simple but smart — nodes that go down come back up quickly, and fall back into their roles with minimal fuss.
  </p>


</div>
<style>
  .post-content {
    max-width: 800px;
    margin: 0 auto;
    padding: 20px;
  }
  
  .post-content h2 {
    font-family: 'Quicksand', sans-serif;
    font-size: 1.8em;
    color: #ff5555;
    margin-top: 1.5em;
    border-bottom: 1px solid #e0e0e0;
    padding-bottom: 0.3em;
  }
  
  .post-content h3 {
    font-family: 'Quicksand', sans-serif;
    font-size: 1.4em;
    color: #333333;
    margin-top: 1.2em;
  }
  
  .post-content ul, 
  .post-content ol {
    margin-left: 1.5em;
    line-height: 1.6;
  }
  
  .post-content li {
    margin-bottom: 0.5em;
  }
  
  .post-content p {
    margin-bottom: 1.2em;
    line-height: 1.6;
  }
</style>