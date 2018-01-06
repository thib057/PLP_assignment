
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxHeight_By_Type {

  public static class TypeTokenizer
       extends Mapper<Object, Text, Text, FloatWritable>{
	private FloatWritable output_value;
	
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String line = value.toString(); //get the line as string
      String[] lineArray = line.split(";"); //get the line as array to retrieve type of tree
      String treeType = lineArray[3]; //tree type
      String treeHeight = lineArray[6]; //get tree height
      Text output_key = new Text(treeType);
      
      //If it is the header, we emit nothing.
      if (treeHeight.equals("HAUTEUR") || treeHeight.isEmpty()){
    	  return;
      }else{
   	   output_value = new FloatWritable(Float.parseFloat(treeHeight));
      };
      
      context.write(output_key, output_value); //write key value
    }
  }

  public static class MaximumFinderReducer
       extends Reducer<Text,FloatWritable,Text,FloatWritable> //keyin, valueIn, keyOut, valueOut
  {
    private FloatWritable maximum_value = new FloatWritable();

    public void reduce(Text key, Iterable<FloatWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      float max = 0;
      for (FloatWritable val : values) {
        if (val.get() > max){
        	max = val.get();
        }
        else{
        	continue;
        }
      }
      maximum_value.set(max);
      context.write(key, maximum_value);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "max height by type");
    job.setJarByClass(MaxHeight_By_Type.class);
    job.setMapperClass(TypeTokenizer.class);
    job.setCombinerClass(MaximumFinderReducer.class);
    job.setReducerClass(MaximumFinderReducer.class);
    job.setOutputKeyClass(Text.class); //output key from mapper is a text class (not string)
    job.setOutputValueClass(FloatWritable.class);
    job.getConfiguration().set("mapreduce.output.basename", "maxheight_by_type"); //rename output file of mapreduce
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileSystem.get(conf).delete(new Path(args[1]),true); //delete outpath if already exists to avoid exception
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
