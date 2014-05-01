package com.avalon.aig.hadoop;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by mannd on 5/1/14.
 */
public class SOLROutputFormat extends FileOutputFormat<NullWritable, Note> {
    @Override
    public RecordWriter<NullWritable, Note> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        final SOLRWriter writer = new SOLRWriter();
        writer.open();

        return new RecordWriter<NullWritable, Note>() {
            public void write(NullWritable key, Note note)
                    throws IOException {
                writer.write(note);
            }

            @Override
            public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
                writer.close();
            }
        };
    }
}