---
layout: page
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