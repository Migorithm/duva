---
layout: post
title:  "Auto completion & suggestion now supported!"
date:   2025-04-15 00:34:01 +0900
categories: posts
---

We’re excited to announce that Duva now supports **autocompletion and suggestion** in its CLI! 🎉 This feature makes it easier and faster to use Duva’s commands, especially for new users or those working with complex commands.

### What’s New?

<video class="demo-video" controls>
  <source src="https://github.com/user-attachments/assets/d14a85a7-5edb-4466-8bd0-f538b3d7d27d" type="video/mp4">
  Your browser does not support the video tag.
</video>


<br>


With autocompletion, you can start typing a command, and Duva will suggest the full syntax, including arguments. For example:

- Type <code>SET</code>, and Duva suggests <code>SET key value [PX milliseconds]</code>.
- Type <code>GET</code>, and it completes to <code>GET key</code> — perfect for quick lookups.

Here’s how it looks in action (suggested text is shown in <span class="suggestion-preview">gray</span>):

<div class="command-example">
<pre>
SET m<span class="suggestion">ykey "value" [PX milliseconds]</span>
</pre>
</div>

<div class="command-example">
<pre>
GET k<span class="suggestion">ey</span>
</pre>
</div>



This feature ensures smooth and responsive suggestions as you type. Whether you’re setting a key with an expiration time or retrieving keys with a pattern, autocompletion has you covered.

### Try It Out

Autocompletion is available for all commands listed in our [Commands documentation](/duva/commands/). Check out the full list to explore commands like <code>SET</code>, <code>GET</code>, <code>KEYS</code>, and <code>SAVE</code>, along with their syntax and examples.



