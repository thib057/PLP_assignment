package mapreduce.sorting.pattern.topkdesign;

import java.io.IOException;
import java.util.TreeMap;

import main.PageRank;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortingMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {
	// Overriding of the map method
	private TreeMap<Double, Text> treem = new TreeMap<Double, Text>(); 
	
	@Override
	protected void map(LongWritable key, Text val, Context context) throws IOException,InterruptedException{
		/* Mapper use the MapReduce sorting module
		 */
		String[] fields = val.toString().split("\\t");
		
		Text nid = new Text(fields[0]);
		double pageRankDouble = Double.parseDouble(fields[2]);
		//pageRank.set(pageRankDouble);
		
		treem.put(pageRankDouble, nid);
		//context.write(pageRank, nid);
		
		if(treem.size() > PageRank.TOP_N_RECORDS) {
			treem.remove(treem.firstKey()); //removes the lowest key value
		}
		
	}
	
	public void run(Context context) throws IOException, InterruptedException {
	    setup(context);
	    while (context.nextKeyValue()) {
	        map(context.getCurrentKey(), context.getCurrentValue(), context);
	    }
	    cleanup(context);
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		for (Double top_pageRank_key : treem.keySet()) {
			Text id = treem.descendingMap().get(top_pageRank_key);
			context.write(new DoubleWritable(top_pageRank_key), id);
		}
	
	}
}
