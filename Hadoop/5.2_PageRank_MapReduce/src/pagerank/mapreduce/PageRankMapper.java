package pagerank.mapreduce;

import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;

public class PageRankMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
	private final static IntWritable nid = new IntWritable(); //current nodeId
	// Overriding of the map method
	@Override
	protected void map(LongWritable key, Text val, Context context) throws IOException,InterruptedException{
		/* This is the beginning of the second job. We have as input:
		 * 
		 * NodeID	link1 link2 link3 ... link_i 	pageRank_of_NodeID
		 * 
		 * With tab between NodeID and links
		 * 
		 * We want here to emit two records :
		 * NodeId	2 	link1 link2 ... link_i
		 * link_i	1 	pageRank_ingoingNode 	number of links to link_i
		 */
			String[] fields = val.toString().split("\\t");
			//if header, returns			
			//check if format is ok
			if(fields.length!=3){
				throw new IOException("Error in input format in step 2");
			}

			String[] adjNodes = fields[1].split(" ");
			nid.set(Integer.parseInt(fields[0]));
			double nodePageRank = Double.parseDouble(fields[2]);
			double newnodePageRank = nodePageRank/adjNodes.length;

			//reconstruct the Node object from the file :
			
			for(int i=0; i<adjNodes.length;i++) {
				//Pass pagerank mass to neighbors at the mean time
				if (!adjNodes[i].equals("null")){ //because some nodes are contain null links list, so we don't want to pass a message
					Text value1_toEmit = new Text("1"+ "\t" + String.valueOf(newnodePageRank) + "\t"+ String.valueOf(adjNodes.length)); //1 in front of the Text object in order to be identified by the reducer
					context.write(new IntWritable(Integer.parseInt(adjNodes[i])), value1_toEmit); //Write node m (link) with pageRank of node n
				}
			}

			context.write(nid, new Text("2"+ "\t" + fields[1])); //second record type, keep graph structure
				    }
	
	public void run(Context context) throws IOException, InterruptedException {
	    setup(context);
	    while (context.nextKeyValue()) {
	        map(context.getCurrentKey(), context.getCurrentValue(), context);
	    }
	    cleanup(context);
	}

}

