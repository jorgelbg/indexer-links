/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.nutch.orion.links;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlink;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.Parse;

import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

/**
 * An {@link org.apache.nutch.indexer.IndexingFilter} that adds
 * <code>outlinks</code> and <code>inlinks</code> field(s) to the document.
 * <p/>
 * This plugins ignores the outlinks that goes to the same host as the URL being indexed. Nevertheless
 * a configuration option to bypass this assumption is available, just add this to your nutch-site.xml
 * <p/>
 * <property>
 * <name>outlinks.host.ignore</name>
 * <value>false</value>
 * </property>
 * <p/>
 * The same checks are done for inlinks, and the same assumption is done, only add those inlinks which
 * host portion is different than the host of the URL.
 * <p/>
 * <property>
 * <name>inlinks.host.ignore</name>
 * <value>false</value>
 * </property>
 *
 * @author Jorge Luis Betancourt González <betancourt.jorge@gmail.com>
 */
public class LinksIndexingFilter implements IndexingFilter {

    public final static String LINKS_OUTLINKS_HOST = "outlinks.host.ignore";
    public final static String LINKS_INLINKS_HOST = "inlinks.host.ignore";
    public final static String LINKS_ONLY_HOSTS = "links.hosts.only";

    public final static org.slf4j.Logger LOG = LoggerFactory.getLogger(LinksIndexingFilter.class);

    private Configuration conf;
    private boolean filterOutlinks;
    private boolean filterInlinks;
    private boolean indexHost;

    // Inherited JavaDoc
    @Override
    public NutchDocument filter(NutchDocument doc, Parse parse, Text url, CrawlDatum datum, Inlinks inlinks)
            throws IndexingException {

        // Add the outlinks
        Outlink[] outlinks = parse.getData().getOutlinks();

        if (indexHost) {
            Set<Outlink> set = new HashSet<Outlink>();
            // This workaround is needed as the Set class is unable of differentiate Outlink objects
            Set<String> hosts = new TreeSet<String>();

            for (Outlink outlink : outlinks) {
                try {
                    String host = new URL(outlink.getToUrl()).getHost();
                    if (!hosts.contains(host)) {
                        hosts.add(host);
                        Outlink link = new Outlink(host, outlink.getAnchor());
                        set.add(link);
                    }
                } catch (MalformedURLException e) {
                    LOG.error("Malformed URL in " + url + ":" + e.getMessage());
                }
            }

            set.toArray(outlinks);
        }

        try {
            if (outlinks != null) {
                for (Outlink outlink : outlinks) {
                    if (filterOutlinks) {
                        String host = new URL(url.toString()).getHost();
                        String outHost = outlink.getToUrl();

                        if (!indexHost) {
                            outHost = new URL(outlink.getToUrl()).getHost();
                        }

                        if (!host.equalsIgnoreCase(outHost)) {
                            doc.add("outlinks", outlink.getToUrl());
                        }
                    } else {
                        doc.add("outlinks", outlink.getToUrl());
                    }
                }
            }
        } catch (MalformedURLException ex) {
            LOG.error("Malformed URL in " + url + ": " + ex.getMessage());
        }
        
        // Add the inlinks, that comes from the reduce portion of the
        // indexing filter
        if (null != inlinks) {
            Iterator<Inlink> iterator = inlinks.iterator();
            Set<String> inlinkHosts = new HashSet<String>();

            while (iterator.hasNext()) {
                Inlink link = iterator.next();
                String linkUrl = link.getFromUrl();

                if (indexHost) {
                    try {
                        linkUrl = new URL(link.getFromUrl()).getHost();

                        if (inlinkHosts.contains(linkUrl)) continue;

                        inlinkHosts.add(linkUrl);
                    } catch (MalformedURLException e) {
                        LOG.error("Malformed URL in " + url + ":" + e.getMessage());
                    }
                }

                try {
                    if (filterInlinks) {
                        String host = new URL(url.toString()).getHost();
                        String inHost = linkUrl;

                        if (!indexHost) {
                            inHost = new URL(link.getFromUrl()).getHost();
                        }

                        if (!host.equalsIgnoreCase(inHost)) {
                            doc.add("inlinks", linkUrl);
                        }
                    } else {
                        doc.add("inlinks", linkUrl);
                    }
                } catch (MalformedURLException e) {
                    LOG.error("Malformed URL in " + url + ":" + e.getMessage());
                }
            }

        }

        return doc;
    }

    /* ----------------------------- *
     * <implementation:Configurable> *
     * ----------------------------- */
    public void setConf(Configuration conf) {
        this.conf = conf;
        filterOutlinks = conf.getBoolean(LINKS_OUTLINKS_HOST, true);
        filterInlinks = conf.getBoolean(LINKS_INLINKS_HOST, true);
        indexHost = conf.getBoolean(LINKS_ONLY_HOSTS, false);
    }

    public Configuration getConf() {
        return this.conf;
    }
    /* ------------------------------ *
     * </implementation:Configurable> *
     * ------------------------------ */
}
