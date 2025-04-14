---
layout: default
title: Commands
permalink: /commands/
---

## Duva Commands


Below is a list of all supported commands in Duva. Click on a command to learn more about its usage and examples.

<ul>
{% for command in site.commands %}
  <li><a href="{{ command.url | relative_url }}">{{ command.title }}</a> - {{ command.description }}</li>
{% endfor %}
</ul>



<div class="autocomplete-note">
  <p>💡 <strong>Pro Tip:</strong> Duva’s CLI supports autocompletion! For example, typing <code>SET</code> suggests <code>key value PX milliseconds</code> to help you work faster.</p>
</div>
