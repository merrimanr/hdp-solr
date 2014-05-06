package com.aig.hadoop;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by mannd on 5/1/14.
 */
public class Note implements WritableComparable<Note> {

    private final Text id = new Text();
    private final Text type = new Text();
    private final Text text = new Text();

  public Note() {

  }

    public Note(String id, String type, String text) {
        setID(id);
        setType(type);
        setText(text);
    }





    void setID(String id) {
        this.id.set(id);
    }

    public String getID() {
        return this.id.toString();
    }

    void setType(String type) {
        this.type.set(type);
    }

    public String getType() {

        return this.type.toString();
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
        row.append(getType()).append("|");
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
        type.write(dataOutput);
        text.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        id.readFields(dataInput);
        type.readFields(dataInput);
        text.readFields(dataInput);
    }
}
