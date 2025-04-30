---
layout: post
title: "Deferred Validation: Safer State-Dependent Writes in Distributed Systems"
date: 2025-04-30 00:00:01 +0900
categories: posts
---

<div class="post-content">
  <h2>Deferred Validation: Safer State-Dependent Writes in Distributed Systems</h2>

  <p>In distributed systems using consensus protocols like Raft, one of the trickiest challenges is handling operations that depend on the current state—like <code>INCR</code>—in a safe and deterministic way.</p>

  <p>In a <a href="/duva/posts/2025/04/15/incr">previous post</a>, we explained how we transformed non-idempotent operations (like <code>INCR</code>) into idempotent ones by performing a read-before-write. While that worked for idempotency, we later realized it introduced subtle race conditions.</p>

  <p>This post explains how we addressed those issues by deferring validation until the log entry is applied to the state machine.</p>

  <h3>The Problem with Pre-Log Validation</h3>

  <p>Previously, we validated commands before writing them to the Raft log. For example, when handling an <code>INCR</code> request, we would:</p>

  <ol>
    <li>Read the current value of the key</li>
    <li>Compute the incremented value</li>
    <li>Transform the request into a <code>SET</code> command</li>
    <li>Write the <code>SET</code> to the log</li>
  </ol>

  <p>This made retries safe and the operation idempotent. However, it introduced correctness problems under concurrency.</p>

  <h4>Race Condition Example</h4>

  <p>Imagine three requests arriving around the same time:</p>

  <ol>
    <li><code>SET x 1</code></li>
    <li><code>INCR x</code> (reads 1, plans to set 2)</li>
    <li><code>SET x "oops"</code></li>
  </ol>

  <p>If these commands are validated before logging, they each appear valid in isolation. But when applied, the actual state may have changed—e.g., <code>INCR</code> could now be applied to a non-numeric string.</p>

  <h3>The Fix: Validate at Apply-Time</h3>

  <p>We changed our system to record the original request in the Raft log without performing any pre-log validation. Then, <strong>validation happens only when the log entry is applied</strong> to the state machine.</p>

<div class="command-example">
  {% highlight rust %}
fn apply_command(cmd: Command) -> Result<(), Error> {
    match cmd {
        Command::Incr(key) => {
            let value = state.get(key);
            match value {
                Some(Value::Number(n)) => {
                    state.set(key, Value::Number(n + 1));
                }
                Some(_) => return Err("Cannot increment non-numeric value".into()),
                None => state.set(key, Value::Number(1)),
            }
        }
        Command::Set(key, value) => {
            state.set(key, value);
        }
    }
}
  {% endhighlight %}
</div>

  <p>By deferring validation, the command sees the <em>actual</em> current state, avoiding stale reads and making operations safer.</p>

  <h3>Why This Works Better</h3>

  <ul>
    <li><strong>Eliminates race conditions:</strong> Operations are validated against the real state.</li>
    <li><strong>Simplifies the Raft log:</strong> We log intent, not precomputed results.</li>
    <li><strong>Keeps clients simple:</strong> No need for clients to read or calculate anything.</li>
  </ul>

  <p>It’s a cleaner model: logs record what the client wants to do, and the state machine determines whether it's valid.</p>

  <h3>Trade-Offs</h3>

  <ul>
    <li><strong>Delayed error reporting:</strong> Invalid operations (e.g., <code>INCR</code> on a string) are only rejected when applied.</li>
    <li><strong>More logic in the state machine:</strong> It now handles all validation.</li>
  </ul>

  <p>Still, the benefits in correctness and concurrency safety outweigh the downsides.</p>

  <h3>Conclusion</h3>

  <p>State-dependent operations like <code>INCR</code> can’t safely rely on pre-log validation. By moving validation to the state machine apply phase, we eliminate race conditions and make our distributed system more robust and predictable.</p>

  <p>From now on, our Raft logs carry only the user’s original intent. The state machine is the sole authority on whether a command is valid—based on the real state at the moment of application.</p>
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

  .post-content h4 {
    font-size: 1.2em;
    color: #666666;
    margin-top: 1em;
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

  .command-example pre {
    background-color: #f5f5f5;
    border-left: 4px solid #ff5555;
    padding: 10px;
    overflow-x: auto;
    font-family: monospace;
    font-size: 0.95em;
    font-color: #4323;
  }
</style>