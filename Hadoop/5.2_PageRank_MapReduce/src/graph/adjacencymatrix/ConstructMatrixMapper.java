package graph.adjacencymatrix;

import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ConstructMatrixMapper extends Mapper<LongWritable, Text, Text, Text> {
	private final static Text in_nid = new Text(); //ingoing node id
	private final static Text out_nid = new Text(); //outgoing node id
	// Overriding of the map method
	@Override
	protected void map(LongWritable key, Text val, Context context) throws IOException,InterruptedException{
		/* We receive the following input format file
		 * <nodeFrom>		<nodeTo>
		 * 
		 * We first emit to reducers each of these file lines, without emiting headers
		 */
			String line = val.toString();
			//if header, returns
			
			if(line.charAt(0) == (char)'#'){
				return;
			}
			
			String[] ids = line.split("\\t"); //to retrieve
			
			//check if format is ok
			if(ids.length!=2){
				throw new IOException("Error in input format");
			}
			
			in_nid.set(ids[0]);
			out_nid.set(ids[1]);

			context.write(in_nid, out_nid);
	    }
	
	
	
	public void run(Context context) throws IOException, InterruptedException {
	    setup(context);
	    while (context.nextKeyValue()) {
	        map(context.getCurrentKey(), context.getCurrentValue(), context);
	    }
	    cleanup(context);
	}

}

