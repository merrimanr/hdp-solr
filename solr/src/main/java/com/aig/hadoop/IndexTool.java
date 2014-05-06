package com.aig.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by rmerriman on 5/1/14.
 */
public class IndexTool extends Configured implements Tool {

  public static class Map extends
          Mapper<LongWritable, Text, NullWritable, Note> {

    private NullWritable nullKey = NullWritable.get();

    @Override
    protected void map(
            LongWritable key,
            Text value,
            Context context)
            throws IOException, InterruptedException {
      System.out.println("value is : " + value.toString());
      String[] parts = value.toString().split("\\|");
      String title = parts[0];
      String type = parts[1];
      String id = parts[2];
      String text = parts[3];
      Note note = new Note(id, type, text);

      context.write(nullKey, note);
    }
  }

  public static class Reduce extends Reducer<IntWritable, IntWritable,
          WritableComparable, HCatRecord> {


    @Override
    protected void reduce(
            IntWritable key,
            Iterable<IntWritable> values,
            Context context)
            throws IOException, InterruptedException {
      int sum = 0;
      Iterator<IntWritable> iter = values.iterator();
      while (iter.hasNext()) {
        sum++;
        iter.next();
      }
      HCatRecord record = new DefaultHCatRecord(2);
      record.set(0, key.get());
      record.set(1, sum);

      context.write(null, record);
    }
  }

  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    Job job = new Job(conf, "IndexTool");
    TextInputFormat.addInputPath(job, new Path("/apps/hive/warehouse/index_record"));
    job.setInputFormatClass(TextInputFormat.class);
    job.setJarByClass(IndexTool.class);
    job.setMapperClass(Map.class);
    job.setNumReduceTasks(0);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Note.class);
    job.setOutputFormatClass(SOLROutputFormat.class);
    return (job.waitForCompletion(true) ? 0 : 1);
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new IndexTool(), args);
    System.exit(exitCode);
  }
}
