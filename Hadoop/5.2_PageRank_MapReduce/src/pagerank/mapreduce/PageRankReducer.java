package pagerank.mapreduce;
import main.PageRank;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;


public class PageRankReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

    @Override
    public void reduce(final IntWritable key, final Iterable<Text> values,
            final Context context) throws IOException, InterruptedException {
    	
    	/* Input from Mapper + Shuffer:
    	 * <nodeId>		2		link1 link2 ... linki	   (TO KEEP GRAPH STRUCTURE)
    	 * <linki> 		1		pageRankOfNodeId 		numberOfOutgoingLinks (of NodeId)
    	 * 
    	 * (SEPARATORS ARE TABS)
    	 * Return :
    	 * 
    	 * <nodeId>		link1 link2 ... linki 	newRank
    	 * 
    	 * REDUCER JOB : we want to calculate the new page Rank value. First term is constant (DUMPING/NB_NODES)
    	 * 				 The second term will be calculated in this part.
    	 */

        Iterator<Text> iterator = values.iterator();
        double sum = 0.0; //second term init
        String links = null;
        System.out.println("Node ID");
        System.out.println(key.toString());
        while (iterator.hasNext()) {
        	String[] records = iterator.next().toString().split("\\t");
        	        	if (Integer.parseInt(records[0]) == 2) {
        		//keep graph structure
        		links = records[1];
        	}else{
        		double pageRank = Double.parseDouble(records[1]);
        		int outDegree_sourceNode = Integer.parseInt(records[2]); 
        		
        		System.out.println("pageRank and outDegree of sourceNode");
                System.out.println(pageRank);
                System.out.println(outDegree_sourceNode);
        		sum+=(pageRank/outDegree_sourceNode);
        		System.out.println("current sum");
        		System.out.println(sum);
        	}
        }
        System.out.println("final sum");
        System.out.println(sum);
        System.out.println("new page rank");
        double newPageRank = (1 - PageRank.DAMPING)/PageRank.NUMBER_OF_NODES + PageRank.DAMPING * sum;

        String output_reducer = links + "\t" + String.valueOf(newPageRank);
        
        //if (links != null){ //CASE Node has no outgoing nodes
        //context.write(key, new Text(output_reducer));
        //}
        
        context.write(key, new Text(output_reducer));

    }
    
    
}
