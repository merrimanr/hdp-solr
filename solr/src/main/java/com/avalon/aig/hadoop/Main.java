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
        String id = "id2";
        String type = "type2";
        String text = "text2";
        Note note = new Note(id, type, text);
        try {
            writer.open();
            writer.write(note);
            writer.close();
        } catch (IOException e) {
            System.out.println("failure");
        }
    }
}
