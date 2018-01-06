
import java.io.IOException;
import java.util.StringTokenizer;
import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Number_By_Type {

  public static class TypeTokenizer
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String line = value.toString(); //get the line as string
      String[] lineArray = line.split(";"); //get the line as array to retrieve type of tree
      String treeType = lineArray[3]; //tree type
      Text output_key = new Text(treeType);
      context.write(output_key, one);
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count tree type");
    job.setJarByClass(Number_By_Type.class);
    job.setMapperClass(TypeTokenizer.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class); //output key from mapper is a text class (not string)
    job.setOutputValueClass(IntWritable.class);
    job.getConfiguration().set("mapreduce.output.basename", "wordcount_by_type"); //rename output file of mapreduce
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileSystem.get(conf).delete(new Path(args[1]),true); //delete outpath if already exists to avoid exception
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
