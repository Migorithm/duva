---
layout: post
title: "LRU again : Slab-Optimized Caching"
date: 2025-06-04 00:10:27 +0400
categories: posts
---
<div class="post-content">
  <h2>Slab-Optimized Caching</h2>

  <p>Our community continually inspires us to refine Duva, and a recent discussion sparked a crucial realization. While our initial LRU cache design offered excellent O(1) performance, it inadvertently exposed a deeper, more pervasive issue: <strong>external memory fragmentation</strong>. Today, I want to dive into the implementation of our <strong>Least Recently Used (LRU) cache</strong>, explore the design choices we made, and explain how it integrates seamlessly and safely with our actor-based architecture, particularly focusing on how we tackle both algorithmic performance and critical memory challenges like fragmentation and cache locality.</p>

  <h3>Why LRU?</h3>
<p>The <strong>LRU cache eviction policy</strong> is a classic for a reason. It's simple, intuitive, and generally effective. When the cache reaches its capacity, the item that hasn't been accessed for the longest time is evicted, making space for new data. This policy assumes that data accessed recently is likely to be accessed again soon, which holds true for many real-world workloads.</p>

  <h3>Performance: Amortized O(1) + enhanced with Slab Allocation</h3>

   <p>Duva's LRU implementation combines a <code>HashMap</code> for key lookups and an <strong>index-based doubly linked list</strong> for ordering. This core design provides <strong>O(1)</strong> performance for every operation: lookups, insertions, removals, and updates. But we didn't stop there. To push the boundaries of memory efficiency and CPU cache locality, we've integrated a custom <strong>slab allocator</strong> for managing our cache nodes. This holistic approach ensures our O(1) operations are not just theoretically fast, but blazing fast in practice due to <strong>eliminated memory fragmentation</strong> and significantly optimized memory access patterns.</p>
   <ul>
      <li><strong>1. GET </strong> </li>
      <li><strong>2. INSERT</strong> </li>
      <li><strong>3. REMOVE</strong> </li>
      <li><strong>4. UPDATE</strong> </li>
   </ul>

  <h3>Slab Allocation: The Foundation for Eliminating Fragmentation and Boosting Cache Locality</h3>

  <p>Conventional doubly linked lists often rely on separate heap allocations for each node, which can lead to significant inefficiencies, especially with frequent additions and removals of small objects. This causes <strong>external memory fragmentation</strong>, where free memory gets scattered in small, unusable chunks, and poor <strong>cache locality</strong>, where related data is spread far apart in memory, slowing down CPU access.</p>

  <p>This is where <strong>slab allocation</strong> helps for our LRU cache. Instead of asking the operating system for tiny, disparate blocks of memory for each cache value, we employ a custom slab allocator that takes precise control of memory for these specific objects.</p>

  <h4>How it works:</h4>
  <ul>
    <li><strong>Contiguous Chunks (Slabs):</strong> Our slab allocator pre-allocates large, contiguous blocks of memory (the "slabs"). Each slab is then internally divided into fixed-size slots, perfectly sized to hold a single <code>CacheNode</code>.</li>
    <li><strong>Index-Based Linked List:</strong> Crucially, the <code>prev</code> and <code>next</code> "pointers" within each <code>CacheNode</code> are not raw memory addresses. Instead, they are <strong>INDICES</strong> into the slab's internal array (<code>Vec</code>). This means our "logical" doubly linked list is built directly within this contiguous memory block.</li>
    <li><strong>Efficient Node Management:</strong> When the LRU cache needs a new node, it requests a slot from our slab allocator, which quickly provides an available index from a pre-existing slab. When a node is removed (evicted), its slot (index) is simply marked as free within the slab, making it immediately available for future reuse without involving the slower system allocator.</li>
  </ul>

  <h4>The Benefits:</h4>
  <ul>
    <li><strong>Drastically Reduced Memory Fragmentation:</strong> By reusing fixed-size slots within slabs, we virtually eliminate external memory fragmentation. This ensures memory remains in larger, more usable chunks, making allocation and deallocation much more predictable and efficient for sustained high-performance operations.</li>
    <li><strong>Superior Cache Locality:</strong> Since many <code>CacheNode</code>s reside within the same or very few contiguous slabs, they are physically close to each other in memory. When the CPU fetches one node, there's a much higher probability that nearby nodes (which are often needed next during list traversals) will also be pulled into the CPU's fast L1/L2 cache. This significantly reduces cache misses and speeds up memory access.</li>
    <li><strong>Lower Allocation Overhead:</strong> Bypassing the general-purpose system allocator for each tiny <code>CacheNode</code> allocation reduces the computational overhead associated with memory management, leading to faster overall operations. This directly contributes to the sustained O(1) performance.</li>
  </ul>


  <h3>Safety in Rust: Leveraging Type System for Robustness</h3>

  <p>Our use of <code>HashMap</code> and a <code>Vec</code>-backed <code>Slab</code> with `usize` indices for the linked list enables this. Rust's strict ownership and borrowing rules prevent common pitfalls like dangling pointers, use-after-free errors, and data races at compile time, providing strong guarantees without sacrificing speed.</p>

  <p>The efficiency gains from our index-based linked list within a slab are achieved not through unsafe pointer manipulation, but by carefully designing the data structure to align with Rust's type system and memory management principles. This approach ensures high performance while maintaining the robust safety and reliability Rust is known for.</p>


  <h3>Conclusion</h3>

  <p>By building an <strong>index-based doubly linked list underpinned by a custom slab allocator</strong>, we’ve engineered a high-performance LRU cache that offers <code>O(1)</code> access patterns with optimal memory utilization, exceptional cache locality, and a dramatic reduction in external memory fragmentation. Thanks to Rust's powerful type system, these gains are achieved with inherent safety, and our actor-based architecture provides an additional layer of concurrency safety. This fusion of performance and correctness is key to a scalable, reliable key-value store.</p>

  <p>If you’re interested, dive into our codebase and join the project!</p>
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