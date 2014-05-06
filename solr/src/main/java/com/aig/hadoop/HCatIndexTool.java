package com.aig.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
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
public class HCatIndexTool extends Configured implements Tool {

  public static class Map extends
          Mapper<WritableComparable, HCatRecord, NullWritable, Note> {

    private NullWritable nullKey = NullWritable.get();

    @Override
    protected void map(
            WritableComparable key,
            HCatRecord value,
            org.apache.hadoop.mapreduce.Mapper<WritableComparable, HCatRecord,
                    NullWritable, Note>.Context context)
            throws IOException, InterruptedException {

      String title = (String) value.get(0);
      String type = (String) value.get(1);
      String id = (String) value.get(2);
      String text = (String) value.get(3);
      Note note = new Note(id, type, text);

      context.write(nullKey, note);
    }
  }

  public static class Reduce extends Reducer<IntWritable, IntWritable,
          WritableComparable, HCatRecord> {


    @Override
    protected void reduce(
            IntWritable key,
            java.lang.Iterable<IntWritable> values,
            org.apache.hadoop.mapreduce.Reducer<IntWritable, IntWritable,
                    WritableComparable, HCatRecord>.Context context)
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



    Job job = new Job(conf, "HCatIndexTool");
    String inputTableName = "index_record";
    String dbName = "default";
    // initialize HCatOutputFormat
    HCatInputFormat.setInput(job, dbName, inputTableName);
    job.setInputFormatClass(HCatInputFormat.class);
    job.setJarByClass(HCatIndexTool.class);
    job.setMapperClass(Map.class);
    job.setNumReduceTasks(0);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Note.class);
    job.setOutputFormatClass(SOLROutputFormat.class);
    return (job.waitForCompletion(true) ? 0 : 1);
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new HCatIndexTool(), args);
    System.exit(exitCode);
  }
}
