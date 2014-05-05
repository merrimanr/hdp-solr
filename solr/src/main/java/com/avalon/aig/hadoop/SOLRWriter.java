package com.avalon.aig.hadoop;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.common.SolrInputDocument;

import javax.security.auth.login.Configuration;
import java.io.IOException;

/**
 * Created by mannd on 5/1/14.
 */
public class SOLRWriter {
    private CloudSolrServer solr;

    public void open() throws IOException {
        solr = new CloudSolrServer("localhost:2181");
        solr.setDefaultCollection("collection1");
    }

    SolrInputDocument convertToSOLR(Note note) {
        final SolrInputDocument inputDoc = new SolrInputDocument();
        inputDoc.addField("id", note.getID());
        inputDoc.addField("type", note.getType());
        inputDoc.addField("note", note.getText());

        inputDoc.setDocumentBoost(1.0f);
        return inputDoc;
    }

    public void write(Note note) throws IOException {
        final SolrInputDocument inputDoc = convertToSOLR(note);
        try {
            solr.add(inputDoc);
        } catch (SolrServerException e) {
            throw makeIOException(e);
        }
    }

    public void flush() throws IOException {
        try {
            solr.commit(false, false);
        } catch (final SolrServerException e) {
            throw makeIOException(e);
        }
    }

    public void close() throws IOException {
        this.flush();
        solr.shutdown();
    }

    private static IOException makeIOException(SolrServerException e) {
        final IOException ioe = new IOException();
        ioe.initCause(e);
        return ioe;
    }
}
