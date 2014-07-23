package org.apache.nutch.orion.links;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlink;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseImpl;
import org.apache.nutch.parse.ParseStatus;
import org.apache.nutch.util.NutchConfiguration;

import junit.framework.TestCase;

public class LinksIndexingFilterTest extends TestCase {

    Configuration conf = NutchConfiguration.create();
    LinksIndexingFilter filter = new LinksIndexingFilter();
    Metadata metadata = new Metadata();

    public void setUp() throws Exception {
        metadata.add(Response.CONTENT_TYPE, "text/html");
        super.setUp();
    }

    private Outlink[] generateOutlinks() throws Exception {
        Outlink[] outlinks = new Outlink[2];

        outlinks[0] = new Outlink("http://www.test.com", "test");
        outlinks[1] = new Outlink("http://www.example.com", "example");

        return outlinks;
    }

    public void testFilterOutlinks() throws Exception {
        filter.setConf(conf);

        Outlink[] outlinks = generateOutlinks();

        NutchDocument doc = filter.filter(new NutchDocument(), new ParseImpl("text", new ParseData(
                new ParseStatus(), "title", outlinks, metadata)), new Text(
                "http://www.example.com/"), new CrawlDatum(), new Inlinks());

        assertEquals("Filter outlinks, allow only those from a different host",
                outlinks[0].getToUrl(), doc.getFieldValue("outlinks"));
    }

    public void testFilterInlinks() throws Exception {
        filter.setConf(conf);

        Inlinks inlinks = new Inlinks();
        inlinks.add(new Inlink("http://www.test.com", "test"));
        inlinks.add(new Inlink("http://www.example.com", "example"));

        NutchDocument doc = filter.filter(new NutchDocument(), new ParseImpl("text", new ParseData(
                new ParseStatus(), "title", new Outlink[0], metadata)), new Text(
                "http://www.example.com/"), new CrawlDatum(), inlinks);

        assertEquals("Filter inlinks, allow only those from a different host",
                "http://www.test.com", doc.getFieldValue("inlinks"));
    }

    public void testNoFilterOutlinks() throws Exception {
        conf.set(LinksIndexingFilter.LINKS_OUTLINKS_HOST, "false");
        filter.setConf(conf);

        Outlink[] outlinks = generateOutlinks();

        NutchDocument doc = filter.filter(new NutchDocument(), new ParseImpl("text", new ParseData(
                new ParseStatus(), "title", outlinks, metadata)), new Text(
                "http://www.example.com/"), new CrawlDatum(), new Inlinks());

        assertEquals("All outlinks must be indexed even those from the same host", outlinks.length,
                doc.getField("outlinks").getValues().size());
    }

    public void testNoFilterInlinks() throws Exception {
        conf.set(LinksIndexingFilter.LINKS_INLINKS_HOST, "false");
        filter.setConf(conf);

        Inlinks inlinks = new Inlinks();
        inlinks.add(new Inlink("http://www.test.com", "test"));
        inlinks.add(new Inlink("http://www.example.com", "example"));

        NutchDocument doc = filter.filter(new NutchDocument(), new ParseImpl("text", new ParseData(
                new ParseStatus(), "title", new Outlink[0], metadata)), new Text(
                "http://www.example.com/"), new CrawlDatum(), inlinks);

        assertEquals("All inlinks must be indexed even those from the same host", inlinks.size(),
                doc.getField("inlinks").getValues().size());
    }
}