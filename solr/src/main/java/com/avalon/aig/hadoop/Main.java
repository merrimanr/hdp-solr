package com.avalon.aig.hadoop;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by mannd on 5/1/14.
 */
public class Main {
    public static void main(String[] args) throws IOException, SolrServerException {
        SOLRWriter writer = new SOLRWriter();
        String id = "id1";
        ArrayList<String> facets = new ArrayList<String>();
        facets.add("facet1");
        facets.add("facet2");
        String text = "text1";
        Note note = new Note(id, facets, text);
        try {
            writer.open();
            writer.write(note);
            writer.close();
        } catch (IOException e) {
            System.out.println("failure");
        }
    }
}
