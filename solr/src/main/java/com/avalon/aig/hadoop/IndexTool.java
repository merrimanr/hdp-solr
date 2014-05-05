package com.avalon.aig.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by rmerriman on 5/1/14.
 */
public class IndexTool extends Configured implements Tool {

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

    String inputTableName = "index_record";
    String dbName = "default";

    Job job = new Job(conf, "IndexTool");
    HCatInputFormat.setInput(job, dbName, inputTableName);
    // initialize HCatOutputFormat

    job.setInputFormatClass(HCatInputFormat.class);
    job.setJarByClass(IndexTool.class);
    job.setMapperClass(Map.class);
    job.setNumReduceTasks(0);
    //job.setReducerClass(Reduce.class);
    //job.setMapOutputKeyClass(NullWritable.class);
    //job.setMapOutputValueClass(Note.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Note.class);
    //job.setOutputValueClass(Text.class);
    //HCatOutputFormat.setOutput(job, OutputJobInfo.create(dbName,
    //        outputTableName, null));
    //HCatSchema s = HCatOutputFormat.getTableSchema(conf);

    //System.err.println("INFO: output schema explicitly set for writing:"
    //        + s);
    //HCatOutputFormat.setSchema(conf, s);
    //job.setOutputFormatClass(HCatOutputFormat.class);
    //TextOutputFormat.setOutputPath(job, new Path("/tmp/results"));
    //job.setOutputFormatClass(TextOutputFormat.class);
    //SOLROutputFormat.setOutputPath(job, new Path("/tmp/results"));
    job.setOutputFormatClass(SOLROutputFormat.class);
    return (job.waitForCompletion(true) ? 0 : 1);
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new IndexTool(), args);
    System.exit(exitCode);
  }
}
