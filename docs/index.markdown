---
layout: default
title: About
permalink: /
---

<div class="content">
  <header>
    <h1>About Duva</h1>
  </header>

  <main>
    <p>
      <strong>Duva</strong> is a distributed cache server built for fast and scalable key-value storage.
      Written in <code>Rust</code> and using the <em>Actor model</em>, it’s designed for high concurrency,
      fault tolerance, and distributed systems.
    </p>

    <div class="autocomplete-note">
      <p>Think of Duva as your blazing-fast, friendly neighborhood cache—lightweight, reliable, and open to everyone.</p>
    </div>

    <h2>Why Duva?</h2>
    <ul>
      <li><strong>High Concurrency</strong>: Handles thousands of concurrent operations.</li>
      <li><strong>Fault Tolerance</strong>: Reliable with failure detection and recovery.</li>
      <li><strong>Scalable</strong>: Works across multiple servers with ease.</li>
    </ul>

    <h2>Key Features</h2>
    <ul>
      <li><strong>Core Commands inspired by <code>Redis</code></strong>: <code>SET</code>, <code>GET</code>, <code>KEYS</code>, <code>SAVE</code> and the like.</li>
      <li><strong>Expiration</strong>: Set time-to-live (TTL) for keys.</li>
      <li><strong>Persistence</strong>: Save data to RDB or Append-Only Files.</li>
      <li><strong>Clustering</strong>: Supports replication, failure detection, and node liveness checks.</li>
      <li><strong>RESP Protocol</strong>: Compatible with Redis-like commands.</li>
    </ul>

    <h2>Quick Links</h2>
    <ul>
      <li><a href="{{ '/commands/' | relative_url }}">Commands</a> – See the full list of commands.</li>
      <li><a href="{{ '/update/' | relative_url }}">Blog</a> – Stay updated with the latest features.</li>
    </ul>

    <h2>Get Involved</h2>
    <p>
      Duva is open-source under the <strong>Apache License 2.0</strong>. Want to contribute?
      Visit the <a href="https://github.com/migorithm/duva" target="_blank">GitHub repository</a> to learn more or submit a pull request!
    </p>
  </main>
</div>
