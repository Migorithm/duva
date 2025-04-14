---
layout: default
title: Blog
permalink: /update/
---

## Duva Blog

Stay updated with the latest news and features in Duva.

<ul>
{% for post in site.categories.posts %}
  <li>
    <a href="{{ post.url | relative_url }}">{{ post.title }}</a> - {{ post.date | date: "%B %-d, %Y" }}
  </li>
{% endfor %}
</ul>