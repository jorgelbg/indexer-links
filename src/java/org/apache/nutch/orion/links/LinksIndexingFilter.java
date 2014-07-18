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
import java.util.Iterator;

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
 *
 * @author Jorge Luis Betancourt González <betancourt.jorge@gmail.com>
 */
public class LinksIndexingFilter implements IndexingFilter {

    public final static String LINKS_OUTLINKS_HOST = "outlinks.host.ignore";

    public final static org.slf4j.Logger LOG = LoggerFactory.getLogger(LinksIndexingFilter.class);

    private Configuration conf;
    private boolean filterOutlinks;

    // Inherited JavaDoc
    @Override
    public NutchDocument filter(NutchDocument doc, Parse parse, Text url, CrawlDatum datum, Inlinks inlinks)
            throws IndexingException {

        // Add the outlinks
        Outlink[] outlinks = parse.getData().getOutlinks();

        try {
            String host = new URL(url.toString()).getHost();

            if (outlinks != null) {
                for (int i = 0; i < outlinks.length; i++) {
                    String outHost = new URL(outlinks[i].getToUrl()).getHost();

                    if (filterOutlinks) {
                        if (!host.equalsIgnoreCase(outHost)) {
                            doc.add("outlinks", outlinks[i].getToUrl());
                        }
                    } else {
                        doc.add("outlinks", outlinks[i].getToUrl());
                    }
                }
            }
        } catch (MalformedURLException ex) {
            LOG.error("Malformed URL in " + url + ":" + ex.getMessage());
        }

        // Add the inlinks, that comes from the reduce portion of the
        // indexing filter
        if (null != inlinks) {
            Iterator<Inlink> iterator = inlinks.iterator();
            while (iterator.hasNext()) {
                doc.add("inlinks", iterator.next().getFromUrl());
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
    }

    public Configuration getConf() {
        return this.conf;
    }
    /* ------------------------------ *
     * </implementation:Configurable> *
     * ------------------------------ */
}
