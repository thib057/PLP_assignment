package round2;

import org.apache.commons.collections.IteratorUtils;
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import org.apache.hadoop.io.IntWritable;

import utils.CompositeThreeElements;
import utils.CompositeTwoELements;
import utils.compositekeyforTFIDF;

/*
 * 
 * 		WRONG WRONG WRONG --> Look at Reducer2bis
 * 
 */
public class Reducer2 extends Reducer<Text, CompositeTwoELements, CompositeTwoELements, CompositeTwoELements> {

    private Text totalWordCount = new Text();

    @Override
    public void reduce(final Text key,Iterable<CompositeTwoELements> values,
            final Context context) throws IOException, InterruptedException {

        int sum = 0;
        Iterator<CompositeTwoELements> iterator = values.iterator();
        ArrayList<CompositeTwoELements> cache = new ArrayList<CompositeTwoELements>();
        
        while (iterator.hasNext()) {
        	
        	CompositeTwoELements next = iterator.next();
        	sum += Integer.parseInt(next.getSecond());
        	cache.add(next); //add to cache for second loop
        }
        
        String sum_str = Integer.toString(sum);
        Iterator<CompositeTwoELements> iterator2 = cache.iterator(); //for second loop
        
        while (iterator2.hasNext()) {
        	CompositeTwoELements it = iterator2.next();
            String word = it.getFirst().toString();
            String wc = it.getSecond().toString();
            CompositeTwoELements key_2 = new CompositeTwoELements(word, key.toString());
            CompositeTwoELements value_2 = new CompositeTwoELements(wc, sum_str);
            context.write(key_2, value_2);
        }
        
    }
}
