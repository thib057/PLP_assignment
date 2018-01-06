
import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import utils.TwoTextWritable; //custom class for multi output

public class OldestTree_Borough {

  public static class Map_BoroughAge
       extends Mapper<Object, Text, IntWritable, TwoTextWritable>{
	  
	  private final static IntWritable one = new IntWritable(1);

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String line = value.toString(); //get the line as string
      String[] lineArray = line.split(";"); //get the line as array to retrieve type of tree
      //we want to get borough and plantation year to send them in the reducer
      String age = lineArray[5];
      String borough = lineArray[1];
      
      //Create an array of Text object to store mutliple values
      TwoTextWritable output_val = new TwoTextWritable();
      
      //to avoid errors in TwoTextWritable class, not write if it is empty
      if (age.length() != 4 || borough.isEmpty()){
    	  return;
      }else{
    
      output_val.set(age,borough);
      context.write(one, output_val);
      }
    }
  }

  public static class getOldestReducer
       extends Reducer<IntWritable,TwoTextWritable,IntWritable,Text> {
    private Text borough = new Text();
    private IntWritable year_writable = new IntWritable();

    public void reduce(IntWritable key, Iterable<TwoTextWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int oldest=3000;
      int year_int;
      String borough_string;
      for (TwoTextWritable val : values) {
    	  
    	String year = val.getFirst();
        //If it is the header, we return and do nothing.
        if (year.equals("ANNEE PLANTATION") || year.isEmpty()){
      	  return;
        }else{
            year_int = Integer.parseInt(year); //get plantation year
            borough_string = val.getSecond();
        };
        //find minimum year
        if (year_int < oldest){
        	oldest = year_int;
        	borough.set(borough_string);
        }
        else{
        	continue;
        }
        
      }
      year_writable.set(oldest);
      context.write(year_writable, borough);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "oldest");
    job.setJarByClass(OldestTree_Borough.class);
    job.setMapperClass(Map_BoroughAge.class);
    //job.setCombinerClass(getOldestReducer.class);
    job.setReducerClass(getOldestReducer.class);
    job.setOutputKeyClass(IntWritable.class); //output key from mapper
    job.setOutputValueClass(TwoTextWritable.class); //REMEMBER TO CHANGE IT !
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileSystem.get(conf).delete(new Path(args[1]),true); //delete outpath if already exists to avoid exception
    job.getConfiguration().set("mapreduce.output.basename", "oldestTree_borough"); //rename output file of mapreduce
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
