package graph.adjacencymatrix;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

import utils.Node;

public class ConstructMatrixReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(final Text key, final Iterable<Text> values,
            final Context context) throws IOException, InterruptedException {

    	/*
    	We emit the preprocessed file, corresponding to the first iteration.
    	
    	We receive the folling from Mappers + Shuffle :
    	
    	<nodeId>		<link1, link2, ..., link3>	
    	
    	Output format is as following :
    	
    	nodeId	link1 link2 ... link_i pagerank_of_nodeId    	
    	
    	*/
    	
        Iterator<Text> iterator = values.iterator();
        Node output = new Node();

        while (iterator.hasNext()) {
        	Integer node = Integer.parseInt(iterator.next().toString());
            output.addNode(node);
        }

        // context.write(key, new IntWritable(sum));
        context.write(key, new Text(output.toString() + "\t" + String.valueOf(output.getPageRank())));
    }
}
