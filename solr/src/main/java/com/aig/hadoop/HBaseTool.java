package com.aig.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
//import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
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
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by rmerriman on 5/1/14.
 */
public class HBaseTool extends Configured implements Tool {

  public static class Map extends
          Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

    private ImmutableBytesWritable row = new ImmutableBytesWritable();
    private final byte[] columnFamily = Bytes.toBytes("f");
    private final byte[] columnName = Bytes.toBytes("v");


    @Override
    protected void map(
            LongWritable key,
            Text value,
            Context context)
            throws IOException, InterruptedException {
      /*
      String title = (String) value.get(0);
      String type = (String) value.get(1);
      String id = (String) value.get(2);
      String text = (String) value.get(3);
      */
      String[] parts = value.toString().split("\\|");
      String title = parts[0];
      String type = parts[1];
      String id = parts[2];
      String text = parts[3];
      Put put = new Put(Bytes.toBytes(id));
      put.add(columnFamily, columnName, Bytes.toBytes(text));
      context.write(row, put);
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
    args = new GenericOptionsParser(conf, args).getRemainingArgs();

    String inputTableName = "index_record";
    String dbName = "default";

    Job job = new Job(conf, "HCatIndexTool");
    //HCatInputFormat.setInput(job, dbName, inputTableName);
    // initialize HCatOutputFormat

    //job.setInputFormatClass(HCatInputFormat.class);
    TextInputFormat.addInputPath(job, new Path("/apps/hive/warehouse/index_record"));
    job.setInputFormatClass(TextInputFormat.class);
    job.setJarByClass(HBaseTool.class);
    job.setMapperClass(Map.class);
    job.setNumReduceTasks(0);
    //job.setReducerClass(Reduce.class);
    //job.setMapOutputKeyClass(NullWritable.class);
    //job.setMapOutputValueClass(Note.class);
    job.setOutputKeyClass(ImmutableBytesWritable.class);
    job.setOutputValueClass(Put.class);
   
    TableMapReduceUtil.initTableReducerJob(
            "index",      // output table
            null,             // reducer class
            job);
    job.setNumReduceTasks(0);

    return (job.waitForCompletion(true) ? 0 : 1);
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new HBaseTool(), args);
    System.exit(exitCode);
  }
}
