package wordcount;

import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import utils.compositekeyforTFIDF;

import java.io.IOException;
import java.util.StringTokenizer;

// To complete according to your problem
public class WordCountMapper extends Mapper<LongWritable, Text, compositekeyforTFIDF, IntWritable> {
	
	private final static IntWritable one = new IntWritable(1);
	// Overriding of the map method
	@Override
	protected void map(LongWritable keyE, Text valE, Context context) throws IOException,InterruptedException
	    {
	        // To complete according to the processing
	        // Processing : keyI = ..., valI = ...
			String line = valE.toString();
			
			StringTokenizer tokenizer = new StringTokenizer(line);
			while(tokenizer.hasMoreTokens())
			{
				String word = tokenizer.nextToken();
				String docId = ((FileSplit)context.getInputSplit()).getPath().toString();
				compositekeyforTFIDF compKey = new compositekeyforTFIDF(word, docId);
				context.write(compKey, one);
			}
	    }
	
	
	
	public void run(Context context) throws IOException, InterruptedException {
	    setup(context);
	    while (context.nextKeyValue()) {
	        map(context.getCurrentKey(), context.getCurrentValue(), context);
	    }
	    cleanup(context);
	}

}






