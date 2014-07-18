links-extractor
===============

Nutch 1.x plugin that allows the inlinks and outlinks of a webpage to be indexed. By default this plugin *ignores*
those outlinks which host matches the host of the webpage being indexed. This behaviour could be bypassed by adding
the following into your `nutch-site.xml`.

```xml
<property>
  <name>outlinks.host.ignore</name>
  <value>false</value>
</property>
```
