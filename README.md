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

The same considerations taken with the outlinks are taken with the inlinks, basically by default only the inlinks coming from a host different than the host of the webpage are indexed, if you want to change this and index all the outlinks you can
do that via the `nutch-site.xml` configuration fil,; just add the following:

```xml
<property>
  <name>inlinks.host.ignore</name>
  <value>false</value>
</property>
```

In case you're only interested in the host portion of the inlinks and outlinks you should enable a behaviour that allows to index only the host part of the URL, by default the full URL is stored.

```xml
<property>
  <name>links.hosts.only</name>
  <value>true</value>
</property>
```