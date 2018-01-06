package DisplayingSorting;

import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import utils.CompositeThreeElements;
import utils.CompositeTwoELements;
import utils.compositekeyforTFIDF;

import java.io.IOException;
import java.util.StringTokenizer;

// To complete according to your problem
public class MapperSorter extends Mapper<LongWritable, Text, IntWritable, CompositeThreeElements> {
	
	private final static IntWritable one = new IntWritable(1);
	// Overriding of the map method
	@Override
	protected void map(LongWritable keyE, Text valE, Context context) throws IOException,InterruptedException
	    {
	        // To complete according to the processing
	        // Processing : keyI = ..., valI = ...
			String line = valE.toString();
			String[] attributes = line.split("\t"); //to retrieve docID, word and wordcount easily according to the doc structure
			String word = attributes[0];
			String docId = attributes[1];
			String tfidf = attributes[2];
			CompositeThreeElements compValue = new CompositeThreeElements(word, docId, tfidf);
			context.write(one, compValue);
	    }
	
	
	
	public void run(Context context) throws IOException, InterruptedException {
	    setup(context);
	    while (context.nextKeyValue()) {
	        map(context.getCurrentKey(), context.getCurrentValue(), context);
	    }
	    cleanup(context);
	}

}
