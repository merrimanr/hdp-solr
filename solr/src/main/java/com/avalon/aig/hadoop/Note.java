package com.avalon.aig.hadoop;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by mannd on 5/1/14.
 */
public class Note implements WritableComparable<Note> {

    private final Text id = new Text();
    private final ArrayWritable facets = new ArrayWritable(Text.class);
    private final Text text = new Text();

    public Note(String id, List<String> facets, String text) {
        setID(id);
        setFacets(facets);
        setText(text);
    }

    void setID(String id) {
        this.id.set(id);
    }

    public String getID() {
        return this.id.toString();
    }

    void setFacets(List<String> facets) {
        int numFacets = facets.size();
        Writable[] facetsWritable = new Text[numFacets];
        for (int i = 0; i < numFacets; i++) {
            facetsWritable[i] = new Text(facets.get(i));
        }
        this.facets.set(facetsWritable);
    }

    public List<String> getFacets() {
        ArrayList<String> output = new ArrayList<String>();
        if (facets.get() == null) {
            return output;
        }
        for (Writable w : this.facets.get()) {
            output.add(w.toString());
        }
        return output;
    }

    void setText(String text) {
        this.text.set(text);
    }

    public String getText() {
        return this.text.toString();
    }

    @Override
    public String toString() {
        StringBuilder row = new StringBuilder();
        row.append(getFacets()).append("|");
        row.append(getText());
        return row.toString();
    }

    @Override
    public int compareTo(Note o) {
        return this.toString().compareTo(o.toString());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        id.write(dataOutput);
        facets.write(dataOutput);
        text.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        id.readFields(dataInput);
        facets.readFields(dataInput);
        text.readFields(dataInput);
    }
}
